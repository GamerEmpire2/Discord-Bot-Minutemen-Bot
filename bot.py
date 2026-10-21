"""
Minutemen faction moderation bot
- Approved-user list (only they can use commands)
- warn / timeout / tempban / ban, with DM to the punished user
- Events + reminders (participants only stored while the event is open)
- Punishments logged to a Google Sheet
- Reports read from a second Google Sheet -> DM to reviewer with action buttons
"""
import asyncio, os, re, time, traceback
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import aiosqlite
import discord
import gspread
from discord import app_commands
from discord.ext import commands, tasks
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.environ["DISCORD_TOKEN"]
GUILD_ID = int(os.environ["GUILD_ID"])
OWNER_IDS = {int(x) for x in os.environ["OWNER_IDS"].split(",") if x.strip()}
REVIEWER_ID = int(os.getenv("REVIEWER_ID") or next(iter(OWNER_IDS)))
LOG_CHANNEL_ID = int(os.getenv("LOG_CHANNEL_ID") or 0)
GOOGLE_CREDS = os.getenv("GOOGLE_CREDS_FILE", "service_account.json")
LOG_SHEET_ID = os.getenv("LOG_SHEET_ID", "")
REPORT_SHEET_ID = os.getenv("REPORT_SHEET_ID", "")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "60"))
TZ = ZoneInfo(os.getenv("TIMEZONE", "America/New_York"))
DB_PATH = os.getenv("DB_PATH", "bot.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS approved_users (
    user_id INTEGER PRIMARY KEY, added_by INTEGER, added_at INTEGER);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, description TEXT,
    starts_at INTEGER, remind_minutes INTEGER DEFAULT 30,
    reminded INTEGER DEFAULT 0, status TEXT DEFAULT 'open', created_by INTEGER);
CREATE TABLE IF NOT EXISTS event_participants (
    event_id INTEGER, user_id INTEGER, PRIMARY KEY (event_id, user_id));
CREATE TABLE IF NOT EXISTS punishments (
    id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, event_id INTEGER,
    type TEXT, reason TEXT, issued_by INTEGER, issued_at INTEGER,
    expires_at INTEGER, active INTEGER DEFAULT 1);
"""

KIND_LABELS = {"warn": "Warning", "timeout": "Timeout", "tempban": "Temporary ban", "ban": "Ban"}


# ----------------------------------------------------------------- helpers
def now_ts() -> int:
    return int(time.time())


def parse_duration(text: str) -> int:
    m = re.fullmatch(r"(\d+)\s*([mhdw])", text.strip().lower())
    if not m:
        raise ValueError("Duration must look like 30m, 2h, 7d or 2w.")
    return int(m[1]) * {"m": 60, "h": 3600, "d": 86400, "w": 604800}[m[2]]


def fmt_duration(seconds: int) -> str:
    for unit, size in (("d", 86400), ("h", 3600), ("m", 60)):
        if seconds >= size and seconds % size == 0:
            return f"{seconds // size}{unit}"
    return f"{seconds}s"


class Bot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.members = True  # enable "Server Members Intent" in the dev portal
        super().__init__(command_prefix="!", intents=intents)
        self.db: aiosqlite.Connection = None  # type: ignore
        self.sent_rows: set[int] = set()  # report rows already DM'd this session

    async def setup_hook(self):
        self.db = await aiosqlite.connect(DB_PATH)
        self.db.row_factory = aiosqlite.Row
        await self.db.executescript(SCHEMA)
        await self.db.commit()
        guild = discord.Object(id=GUILD_ID)
        self.tree.copy_global_to(guild=guild)
        await self.tree.sync(guild=guild)
        self.expiry_loop.start()
        self.reminder_loop.start()
        if REPORT_SHEET_ID:
            self.report_loop.change_interval(seconds=POLL_SECONDS)
            self.report_loop.start()

    @property
    def guild_obj(self) -> discord.Guild:
        return self.get_guild(GUILD_ID)  # type: ignore

    # ---- background: lift expired tempbans
    @tasks.loop(minutes=1)
    async def expiry_loop(self):
        async with self.db.execute(
            "SELECT id, user_id FROM punishments WHERE type='tempban' AND active=1 AND expires_at<=?",
            (now_ts(),),
        ) as cur:
            rows = await cur.fetchall()
        for r in rows:
            try:
                await self.guild_obj.unban(discord.Object(id=r["user_id"]), reason="Tempban expired")
            except discord.NotFound:
                pass  # already unbanned
            except discord.HTTPException as e:
                print("Unban failed:", e)
                continue
            await self.db.execute("UPDATE punishments SET active=0 WHERE id=?", (r["id"],))
        await self.db.commit()

    # ---- background: event reminders
    @tasks.loop(minutes=1)
    async def reminder_loop(self):
        async with self.db.execute(
            "SELECT * FROM events WHERE status='open' AND reminded=0 "
            "AND starts_at - remind_minutes*60 <= ?",
            (now_ts(),),
        ) as cur:
            events = await cur.fetchall()
        for ev in events:
            async with self.db.execute(
                "SELECT user_id FROM event_participants WHERE event_id=?", (ev["id"],)
            ) as cur:
                people = await cur.fetchall()
            if ev["starts_at"] > now_ts() - 3600:  # don't spam for long-past events
                for p in people:
                    try:
                        user = await self.fetch_user(p["user_id"])
                        await user.send(
                            f"⏰ **Reminder:** {ev['name']} starts <t:{ev['starts_at']}:R> "
                            f"(<t:{ev['starts_at']}:F>)."
                            + (f"\n{ev['description']}" if ev["description"] else "")
                        )
                    except discord.HTTPException:
                        pass
            await self.db.execute("UPDATE events SET reminded=1 WHERE id=?", (ev["id"],))
        await self.db.commit()

    # ---- background: poll the reports sheet
    @tasks.loop(seconds=60)
    async def report_loop(self):
        try:
            reports = await asyncio.to_thread(fetch_pending_reports)
        except Exception as e:
            print("Report sheet error:", e)
            return
        reviewer = await self.fetch_user(REVIEWER_ID)
        for rep in reports:
            if rep["row"] in self.sent_rows:
                continue
            embed = discord.Embed(title=f"Report to review (row {rep['row']})", colour=discord.Colour.orange())
            embed.add_field(name="Reporter", value=rep["reporter"] or "—", inline=False)
            embed.add_field(
                name="Reported user",
                value=f"<@{rep['reported_id']}> (`{rep['reported_id']}`)" if rep["reported_id"] else "No valid ID found",
                inline=False,
            )
            embed.add_field(name="Reason", value=(rep["reason"] or "—")[:1000], inline=False)
            embed.add_field(name="Evidence", value=(rep["evidence"] or "—")[:1000], inline=False)
            embed.set_footer(text=f"Submitted: {rep['timestamp']}")
            view = ReviewView(rep) if rep["reported_id"] else None
            try:
                await reviewer.send(embed=embed, view=view)
            except discord.HTTPException as e:
                print("Could not DM reviewer:", e)
                return
            self.sent_rows.add(rep["row"])
            status = "Awaiting review" if view else "Needs manual review (no valid ID)"
            await asyncio.to_thread(set_report_status, rep["row"], status)


bot = Bot()


# ------------------------------------------------------------ Google Sheets
def gc() -> gspread.Client:
    return gspread.service_account(filename=GOOGLE_CREDS)


def log_to_sheet(row: list):
    if not LOG_SHEET_ID:
        return
    gc().open_by_key(LOG_SHEET_ID).sheet1.append_row(row, value_input_option="USER_ENTERED")


def fetch_pending_reports() -> list[dict]:
    ws = gc().open_by_key(REPORT_SHEET_ID).sheet1
    values = ws.get_all_values()
    if not values:
        return []
    header = [h.strip().lower() for h in values[0]]
    need = ["timestamp", "reporter", "reported user id", "reason", "evidence", "status"]
    missing = [n for n in need if n not in header]
    if missing:
        raise RuntimeError(f"Report sheet is missing columns: {missing}")
    ix = {n: header.index(n) for n in need}
    out = []
    for n, row in enumerate(values[1:], start=2):
        row = row + [""] * (len(header) - len(row))
        if row[ix["status"]].strip():
            continue
        m = re.search(r"\d{15,20}", row[ix["reported user id"]])
        out.append({
            "row": n,
            "timestamp": row[ix["timestamp"]],
            "reporter": row[ix["reporter"]],
            "reported_id": int(m[0]) if m else None,
            "reason": row[ix["reason"]],
            "evidence": row[ix["evidence"]],
        })
    return out


def set_report_status(row: int, text: str):
    ws = gc().open_by_key(REPORT_SHEET_ID).sheet1
    col = [h.strip().lower() for h in ws.row_values(1)].index("status") + 1
    ws.update_cell(row, col, text)


# -------------------------------------------------------------- permissions
async def is_approved_id(user_id: int) -> bool:
    if user_id in OWNER_IDS:
        return True
    async with bot.db.execute("SELECT 1 FROM approved_users WHERE user_id=?", (user_id,)) as cur:
        return await cur.fetchone() is not None


def approved_only():
    async def predicate(interaction: discord.Interaction) -> bool:
        return await is_approved_id(interaction.user.id)
    return app_commands.check(predicate)


def owner_only():
    async def predicate(interaction: discord.Interaction) -> bool:
        return interaction.user.id in OWNER_IDS
    return app_commands.check(predicate)


@bot.tree.error
async def on_app_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.CheckFailure):
        msg = "You're not allowed to use that command."
    else:
        original = getattr(error, "original", error)
        traceback.print_exception(type(original), original, original.__traceback__)
        msg = f"Something went wrong: {original}"
    if interaction.response.is_done():
        await interaction.followup.send(msg, ephemeral=True)
    else:
        await interaction.response.send_message(msg, ephemeral=True)


# --------------------------------------------------------------- punishments
async def apply_punishment(
    kind: str, user_id: int, reason: str, issuer_id: int,
    duration: int | None = None, event_id: int | None = None,
) -> tuple[int, bool]:
    """Apply + record + log a punishment. Returns (punishment_id, dm_delivered)."""
    guild = bot.guild_obj
    if user_id in OWNER_IDS or user_id == bot.user.id:
        raise ValueError("You can't punish that user.")
    member = guild.get_member(user_id)
    if member and member.top_role >= guild.me.top_role:
        raise ValueError("That user's top role is above (or equal to) the bot's role.")
    if kind == "timeout" and not member:
        raise ValueError("That user isn't in the server, so they can't be timed out.")
    if kind in ("timeout", "tempban") and not duration:
        raise ValueError("A duration is required.")
    if kind == "timeout" and duration > 28 * 86400:
        raise ValueError("Discord timeouts max out at 28 days. Use a tempban instead.")

    issued = now_ts()
    expires = issued + duration if duration else None

    # DM first - banned users usually can't be messaged afterwards
    try:
        user = member or await bot.fetch_user(user_id)
    except discord.NotFound:
        raise ValueError("No Discord user with that ID.")
    dm = discord.Embed(title=f"You received a {KIND_LABELS[kind].lower()}", colour=discord.Colour.red())
    dm.add_field(name="Server", value=guild.name, inline=False)
    dm.add_field(name="Reason", value=reason, inline=False)
    if expires:
        dm.add_field(name="Expires", value=f"<t:{expires}:F> (<t:{expires}:R>)", inline=False)
    elif kind == "ban":
        dm.add_field(name="Duration", value="Permanent", inline=False)
    try:
        await user.send(embed=dm)
        dm_ok = True
    except discord.HTTPException:
        dm_ok = False

    audit = f"{reason} (by {issuer_id})"
    if kind == "timeout":
        await member.timeout(timedelta(seconds=duration), reason=audit)  # type: ignore
    elif kind in ("tempban", "ban"):
        await guild.ban(discord.Object(id=user_id), reason=audit)

    cur = await bot.db.execute(
        "INSERT INTO punishments (user_id,event_id,type,reason,issued_by,issued_at,expires_at) "
        "VALUES (?,?,?,?,?,?,?)",
        (user_id, event_id, kind, reason, issuer_id, issued, expires),
    )
    await bot.db.commit()
    pid = cur.lastrowid

    try:
        await asyncio.to_thread(
            log_to_sheet,
            [datetime.fromtimestamp(issued, timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
             pid, kind, str(user_id), reason, fmt_duration(duration) if duration else "",
             str(issuer_id), event_id or ""],
        )
    except Exception as e:
        print("Sheet log failed:", e)

    if LOG_CHANNEL_ID:
        ch = bot.get_channel(LOG_CHANNEL_ID)
        if ch:
            await ch.send(f"**{KIND_LABELS[kind]}** #{pid} | <@{user_id}> | by <@{issuer_id}> | {reason}")
    return pid, dm_ok


async def check_event_membership(event_id: int | None, user_id: int):
    if event_id is None:
        return
    async with bot.db.execute(
        "SELECT 1 FROM event_participants WHERE event_id=? AND user_id=?", (event_id, user_id)
    ) as cur:
        if not await cur.fetchone():
            raise ValueError("That user isn't a participant of that event.")


async def run_punish_command(interaction, user: discord.User, kind, reason, duration_text=None, event_id=None):
    await interaction.response.defer(ephemeral=True)
    try:
        duration = parse_duration(duration_text) if duration_text else None
        await check_event_membership(event_id, user.id)
        pid, dm_ok = await apply_punishment(kind, user.id, reason, interaction.user.id, duration, event_id)
    except (ValueError, discord.HTTPException) as e:
        return await interaction.followup.send(f"❌ {e}", ephemeral=True)
    await interaction.followup.send(
        f"✅ {KIND_LABELS[kind]} #{pid} applied to {user.mention}."
        + ("" if dm_ok else " (Couldn't DM them - their DMs are closed.)"),
        ephemeral=True,
    )


@bot.tree.command(description="Warn a user")
@approved_only()
async def warn(i: discord.Interaction, user: discord.User, reason: str, event_id: int | None = None):
    await run_punish_command(i, user, "warn", reason, None, event_id)


@bot.tree.command(description="Time a user out (e.g. 30m, 2h, 3d)")
@approved_only()
async def timeout(i: discord.Interaction, user: discord.User, duration: str, reason: str, event_id: int | None = None):
    await run_punish_command(i, user, "timeout", reason, duration, event_id)


@bot.tree.command(description="Temporarily ban a user (e.g. 7d, 2w)")
@approved_only()
async def tempban(i: discord.Interaction, user: discord.User, duration: str, reason: str, event_id: int | None = None):
    await run_punish_command(i, user, "tempban", reason, duration, event_id)


@bot.tree.command(description="Permanently ban a user")
@approved_only()
async def ban(i: discord.Interaction, user: discord.User, reason: str, event_id: int | None = None):
    await run_punish_command(i, user, "ban", reason, None, event_id)


@bot.tree.command(description="Show a user's punishment history")
@approved_only()
async def history(i: discord.Interaction, user: discord.User):
    async with bot.db.execute(
        "SELECT * FROM punishments WHERE user_id=? ORDER BY id DESC LIMIT 15", (user.id,)
    ) as cur:
        rows = await cur.fetchall()
    if not rows:
        return await i.response.send_message(f"No punishments on record for {user.mention}.", ephemeral=True)
    lines = [f"#{r['id']} **{r['type']}** <t:{r['issued_at']}:d> by <@{r['issued_by']}> - {r['reason']}" for r in rows]
    await i.response.send_message("\n".join(lines)[:1900], ephemeral=True)


# ------------------------------------------------------- approved user list
@bot.tree.command(description="Add a user to the approved list (owners only)")
@owner_only()
async def approve_add(i: discord.Interaction, user: discord.User):
    await bot.db.execute(
        "INSERT OR IGNORE INTO approved_users VALUES (?,?,?)", (user.id, i.user.id, now_ts())
    )
    await bot.db.commit()
    await i.response.send_message(f"✅ {user.mention} can now use the bot.", ephemeral=True)


@bot.tree.command(description="Remove a user from the approved list (owners only)")
@owner_only()
async def approve_remove(i: discord.Interaction, user: discord.User):
    await bot.db.execute("DELETE FROM approved_users WHERE user_id=?", (user.id,))
    await bot.db.commit()
    await i.response.send_message(f"✅ {user.mention} removed.", ephemeral=True)


@bot.tree.command(description="List approved users")
@approved_only()
async def approve_list(i: discord.Interaction):
    async with bot.db.execute("SELECT user_id FROM approved_users") as cur:
        rows = await cur.fetchall()
    people = [f"<@{o}> (owner)" for o in OWNER_IDS] + [f"<@{r['user_id']}>" for r in rows]
    await i.response.send_message("**Approved:**\n" + "\n".join(people), ephemeral=True)


# ------------------------------------------------------------------- events
@bot.tree.command(description="Create an event. Time format: YYYY-MM-DD HH:MM (your local time)")
@approved_only()
async def event_create(
    i: discord.Interaction, name: str, when: str,
    description: str = "", remind_minutes: int = 30,
):
    try:
        local = datetime.strptime(when, "%Y-%m-%d %H:%M").replace(tzinfo=TZ)
    except ValueError:
        return await i.response.send_message("Use the format `YYYY-MM-DD HH:MM`, e.g. `2026-11-01 20:00`.", ephemeral=True)
    ts = int(local.timestamp())
    cur = await bot.db.execute(
        "INSERT INTO events (name,description,starts_at,remind_minutes,created_by) VALUES (?,?,?,?,?)",
        (name, description, ts, remind_minutes, i.user.id),
    )
    await bot.db.commit()
    await i.response.send_message(f"✅ Event **{name}** created (ID `{cur.lastrowid}`) for <t:{ts}:F>.", ephemeral=True)


@bot.tree.command(description="Add a participant to an event")
@approved_only()
async def event_add(i: discord.Interaction, event_id: int, user: discord.User):
    async with bot.db.execute("SELECT 1 FROM events WHERE id=? AND status='open'", (event_id,)) as cur:
        if not await cur.fetchone():
            return await i.response.send_message("No open event with that ID.", ephemeral=True)
    await bot.db.execute("INSERT OR IGNORE INTO event_participants VALUES (?,?)", (event_id, user.id))
    await bot.db.commit()
    await i.response.send_message(f"✅ {user.mention} added to event `{event_id}`.", ephemeral=True)


@bot.tree.command(description="Remove a participant from an event")
@approved_only()
async def event_remove(i: discord.Interaction, event_id: int, user: discord.User):
    await bot.db.execute("DELETE FROM event_participants WHERE event_id=? AND user_id=?", (event_id, user.id))
    await bot.db.commit()
    await i.response.send_message(f"✅ {user.mention} removed from event `{event_id}`.", ephemeral=True)


@bot.tree.command(description="List open events")
@approved_only()
async def event_list(i: discord.Interaction):
    async with bot.db.execute("SELECT * FROM events WHERE status='open' ORDER BY starts_at") as cur:
        rows = await cur.fetchall()
    if not rows:
        return await i.response.send_message("No open events.", ephemeral=True)
    await i.response.send_message(
        "\n".join(f"`{r['id']}` **{r['name']}** - <t:{r['starts_at']}:F>" for r in rows), ephemeral=True
    )


@bot.tree.command(description="Show an event and its participants")
@approved_only()
async def event_info(i: discord.Interaction, event_id: int):
    async with bot.db.execute("SELECT * FROM events WHERE id=?", (event_id,)) as cur:
        ev = await cur.fetchone()
    if not ev:
        return await i.response.send_message("No event with that ID.", ephemeral=True)
    async with bot.db.execute("SELECT user_id FROM event_participants WHERE event_id=?", (event_id,)) as cur:
        people = await cur.fetchall()
    embed = discord.Embed(title=ev["name"], description=ev["description"] or None)
    embed.add_field(name="When", value=f"<t:{ev['starts_at']}:F>")
    embed.add_field(name="Status", value=ev["status"])
    embed.add_field(name="Participants", value="\n".join(f"<@{p['user_id']}>" for p in people) or "None", inline=False)
    await i.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(description="End an event and forget its participants")
@approved_only()
async def event_end(i: discord.Interaction, event_id: int):
    await bot.db.execute("DELETE FROM event_participants WHERE event_id=?", (event_id,))
    await bot.db.execute("UPDATE events SET status='ended' WHERE id=?", (event_id,))
    await bot.db.commit()
    await i.response.send_message(f"✅ Event `{event_id}` ended; participant list deleted.", ephemeral=True)


# ---------------------------------------------------------- report review UI
class ReviewView(discord.ui.View):
    def __init__(self, report: dict):
        super().__init__(timeout=None)
        self.report = report

    async def _act(self, interaction: discord.Interaction, kind: str | None, duration: str | None = None):
        if not await is_approved_id(interaction.user.id):
            return await interaction.response.send_message("Not allowed.", ephemeral=True)
        await interaction.response.defer()
        row = self.report["row"]
        try:
            if kind is None:
                status = f"Dismissed by {interaction.user}"
                result = "Dismissed."
            else:
                secs = parse_duration(duration) if duration else None
                pid, _ = await apply_punishment(
                    kind, self.report["reported_id"],
                    f"Report row {row}: {self.report['reason']}"[:500],
                    interaction.user.id, secs,
                )
                status = f"Approved - {kind} (#{pid}) by {interaction.user}"
                result = f"✅ {KIND_LABELS[kind]} #{pid} applied."
        except (ValueError, discord.HTTPException) as e:
            return await interaction.followup.send(f"❌ {e}")
        for child in self.children:
            child.disabled = True  # type: ignore
        await interaction.edit_original_response(view=self)
        await interaction.followup.send(result)
        await asyncio.to_thread(set_report_status, row, status)

    @discord.ui.button(label="Warn", style=discord.ButtonStyle.primary)
    async def b_warn(self, i: discord.Interaction, _):
        await self._act(i, "warn")

    @discord.ui.button(label="Timeout 1h", style=discord.ButtonStyle.secondary)
    async def b_timeout(self, i: discord.Interaction, _):
        await self._act(i, "timeout", "1h")

    @discord.ui.button(label="Tempban 7d", style=discord.ButtonStyle.danger)
    async def b_tempban(self, i: discord.Interaction, _):
        await self._act(i, "tempban", "7d")

    @discord.ui.button(label="Ban", style=discord.ButtonStyle.danger)
    async def b_ban(self, i: discord.Interaction, _):
        await self._act(i, "ban")

    @discord.ui.button(label="Dismiss", style=discord.ButtonStyle.success)
    async def b_dismiss(self, i: discord.Interaction, _):
        await self._act(i, None)


if __name__ == "__main__":
    bot.run(TOKEN)
