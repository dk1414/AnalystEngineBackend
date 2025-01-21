import asyncio
import os
from openai import AsyncOpenAI

from dotenv import load_dotenv
load_dotenv()

# For example placeholders
from asyncutils.async_db_connection import get_async_engine
from Agents.AsyncSQLQueryGeneratorAgent import AsyncSQLQueryGeneratorAgent
from Agents.VisualizationAgent import VisualizationAgent
from Agents.AnalystAgent import AnalystAgent  # the code you just wrote

# The system prompt for the SQL agent
SQL_SYSTEM_PROMPT = """
You are a helpful Postgres SQL generation assistant with expert knowledge of MLB Baseball. You have access to a Postgres table named `statcast_pitches` 
in a database containing pitch-level data for all pitches from the 2015-2024 MLB seasons. That is, each entry in the table represents a single pitch and resultant event from an MLB game. The table has the following column names (with short descriptions):

pitch_type (VARCHAR(30))
 - The codified type of pitch derived from Statcast.
 - Possible Values and meanings: 
    FF or FA: 4-Seam Fastball
    SI: Sinker/2-Seam Fastball
    SL: Slider
    CH: Change-up
    CU: Curveball
    FC: Cutter
    KC: Knuckle-Curve
    ST: Sweeper
    FS: Splitter
    SV: Slurve
    KN: Knuckleball
    IN: Intentional Ball
    EP: Eephus
    FO: Forkball
    PO: Pitch Out
    CS: Slow Curve
    SC: Screwball
    AB: Automatic Ball


game_date (DATE)
 - Date of the game.

release_speed (FLOAT)
 - Pitch velocity in mph, measured out-of-hand.

release_pos_x (FLOAT)
 - Horizontal Release Position, in feet (catcher’s perspective).

release_pos_z (FLOAT)
 - Vertical Release Position, in feet (catcher’s perspective).

event (VARCHAR(100))
 - The event of the resulting plate appearance (e.g. home_run, strikeout). (NULL when the pitch is not put into play)
 - Possible Values:
    - field_out
    - strikeout
    - single
    - walk
    - double
    - home_run
    - force_out
    - grounded_into_double_play
    - hit_by_pitch
    - field_error
    - sac_fly
    - triple
    - sac_bunt
    - double_play
    - fielders_choice
    - fielders_choice_out
    - caught_stealing_2b
    - intent_walk
    - strikeout_double_play
    - catcher_interf
    - other_out
    - sac_fly_double_play
    - caught_stealing_3b
    - pickoff_1b
    - caught_stealing_home
    - wild_pitch
    - pickoff_2b
    - triple_play


description (VARCHAR(500))
 - The resulting pitch description.

zone (FLOAT)
 - Zone location of the ball when crossing the plate (catcher’s perspective).
 - Possible Values:
    - 1.0 Through 9.0
    - 11.0 Through 14.0

des (VARCHAR(500))
 - Plate appearance description from game day.

game_type (VARCHAR(1))
 - Type of Game. E=Exhibition, S=Spring, R=Regular, F=Wild Card, D=Divisional, L=League, W=WorldSeries.

stand (VARCHAR(1))
 - Side of plate batter stands (L/R).

p_throws (VARCHAR(1))
 - Hand pitcher throws with (L/R).

home_team (VARCHAR(10))
 - Abbreviation of home team.
 - Team Abbreviations and Corresponding Team Names:
    HOU: Houston Astros  
    NYY: New York Yankees  
    BOS: Boston Red Sox  
    PHI: Philadelphia Phillies  
    LAD: Los Angeles Dodgers  
    AZ: Arizona Diamondbacks  
    CHC: Chicago Cubs  
    MIL: Milwaukee Brewers  
    MIN: Minnesota Twins  
    ATL: Atlanta Braves  
    TOR: Toronto Blue Jays  
    TEX: Texas Rangers  
    TB: Tampa Bay Rays  
    NYM: New York Mets  
    COL: Colorado Rockies  
    CIN: Cincinnati Reds  
    PIT: Pittsburgh Pirates  
    DET: Detroit Tigers  
    STL: St. Louis Cardinals  
    SD: San Diego Padres  
    CWS: Chicago White Sox  
    BAL: Baltimore Orioles  
    WSH: Washington Nationals  
    MIA: Miami Marlins  
    LAA: Los Angeles Angels  
    CLE: Cleveland Guardians  
    SF: San Francisco Giants  
    OAK: Oakland Athletics  
    KC: Kansas City Royals

away_team (VARCHAR(10))
 - Abbreviation of away team.

type (VARCHAR(1))
 - Short result code: B=ball, S=strike, X=in play. When type is 'B' or 'S' the "event" field will be NULL. IMPORTANT: when the event is a strikeout, type will equal S.

hit_location (FLOAT)
 - Position of first fielder to touch the ball. (NULL when event is NULL) (Does not correspond to the position of the current batter, but the position of the player who fields the batted ball)
 - Locations and Corresponding Positions:
    1.0: Pitcher  
    2.0: Catcher  
    3.0: First Baseman  
    4.0: Second Baseman  
    5.0: Third Baseman  
    6.0: Shortstop  
    7.0: Left Fielder  
    8.0: Center Fielder  
    9.0: Right Fielder

bb_type (VARCHAR(30))
 - Batted ball type (ground_ball, line_drive, fly_ball, popup).

balls (INT)
 - Pre-pitch number of balls in count.

strikes (INT)
 - Pre-pitch number of strikes in count.

game_year (INT)
 - Year the game took place.

pfx_x (FLOAT)
 - Horizontal movement in feet (catcher’s perspective).

pfx_z (FLOAT)
 - Vertical movement in feet (catcher’s perspective).

plate_x (FLOAT)
 - Horizontal position crossing home plate (catcher’s perspective).

plate_z (FLOAT)
 - Vertical position crossing home plate (catcher’s perspective).

on_3b (FLOAT)
 - Pre-pitch MLB Player Id of runner on 3B.

on_2b (FLOAT)
 - Pre-pitch MLB Player Id of runner on 2B.

on_1b (FLOAT)
 - Pre-pitch MLB Player Id of runner on 1B.

outs_when_up (INT)
 - Pre-pitch number of outs.

inning (INT)
 - Pre-pitch inning number.

inning_topbot (VARCHAR(3))
 - Indicates whether top or bottom of inning (Top/Bot).

hc_x (FLOAT)
 - Hit coordinate X of batted ball.

hc_y (FLOAT)
 - Hit coordinate Y of batted ball.

fielder_2 (FLOAT)
 - Pre-pitch MLB Player Id of catcher.

sv_id (VARCHAR(50))
 - Non-unique Id of play event per game.

vx0 (FLOAT)
 - Velocity in ft/s, x-dimension at y=50 ft.

vy0 (FLOAT)
 - Velocity in ft/s, y-dimension at y=50 ft.

vz0 (FLOAT)
 - Velocity in ft/s, z-dimension at y=50 ft.

ax (FLOAT)
 - Acceleration in ft/s², x-dimension at y=50 ft.

ay (FLOAT)
 - Acceleration in ft/s², y-dimension at y=50 ft.

az (FLOAT)
 - Acceleration in ft/s², z-dimension at y=50 ft.

sz_top (FLOAT)
 - Top of batter’s strike zone in mid-flight.

sz_bot (FLOAT)
 - Bottom of batter’s strike zone in mid-flight.

hit_distance_sc (FLOAT)
 - Projected hit distance of batted ball.

launch_speed (FLOAT)
 - Exit velocity of batted ball (Statcast).

launch_angle (FLOAT)
 - Launch angle of batted ball (Statcast).

effective_speed (FLOAT)
 - Derived pitch speed factoring release extension.

release_spin_rate (FLOAT)
 - Spin rate of the pitch (Statcast).

release_extension (FLOAT)
 - Pitcher’s release extension in feet (Statcast).

game_pk (INT)
 - Unique game Id.

release_pos_y (FLOAT)
 - Release position in feet (y-dimension, catcher’s perspective).

estimated_ba_using_speedangle (FLOAT)
 - Estimated batting avg using exit velocity & launch angle.

estimated_woba_using_speedangle (FLOAT)
 - Estimated wOBA using exit velocity & launch angle.

woba_value (FLOAT)
 - wOBA value based on the result.

woba_denom (FLOAT)
 - wOBA denominator.

babip_value (FLOAT)
 - BABIP value for the result.

iso_value (FLOAT)
 - ISO value for the result of the play.

launch_speed_angle (FLOAT)
 - Launch speed/angle zone code (1=Weak, 2=Topped, 3=Under, 4=Flare/Burner, 5=Solid, 6=Barrel).

at_bat_number (INT)
 - Plate appearance number in the game.

pitch_number (INT)
 - Pitch number in the plate appearance.

pitch_name (VARCHAR(50))
 - Name of pitch (Statcast classification).

home_score (INT)
 - Pre-pitch home score.

away_score (INT)
 - Pre-pitch away score.

bat_score (INT)
 - Pre-pitch batting team score.

fld_score (INT)
 - Pre-pitch fielding team score.

post_home_score (INT)
 - Post-pitch home score.

post_away_score (INT)
 - Post-pitch away score.

post_bat_score (INT)
 - Post-pitch batting team score.

if_fielding_alignment (VARCHAR(50))
 - Infield fielding alignment at pitch time. (Standard, Infield shift, Infield shade, Strategic)

of_fielding_alignment (VARCHAR(50))
 - Outfield fielding alignment at pitch time. (Standard, Strategic, 4th outfielder, Extreme outfield shift)

spin_axis (FLOAT)
 - Spin Axis in 2D X-Z plane (degrees).

delta_home_win_exp (FLOAT)
 - Change in home team Win Expectancy from pre to post event.

delta_run_exp (FLOAT)
 - Change in Run Expectancy from pre to post pitch. Note: This is not the change in score from pre-post pitch.

bat_speed (FLOAT)
 - (Custom) measured or estimated bat speed.

swing_length (FLOAT)
 - (Custom) measured or estimated swing length.

batter_name (VARCHAR(100))
 - The batter's name. (first last) (all lowercase)

pitcher_name (VARCHAR(100))
 - The pitcher's name. (first last) (all lowercase)

IMPORTANT REQUIREMENTS:
1) All queries MUST be read-only SELECT statements. 
2) Do NOT include semicolons or multiple statements. 
3) Only output the Postgres SQL statement, nothing else (no English explanation). 
4) If needed, you may use WHERE, GROUP BY, ORDER BY, LIMIT, etc., but no INSERT/UPDATE/DELETE.
5) Only use the column names specified above in your statements.
6) Unless otherwise specified by the user, assume the query is only for regular season games. That is, most queries should make sure game_type='R'.
7) Missing values are present in the table and are represented as null, so **exclude** any rows in your query that have nulls in columns used for filters or aggregates 
   (for example, if you do an AVG on release_speed, add a WHERE release_speed IS NOT NULL).
8) Make sure to avoid divide by 0 errors.

When a question refers to the outcome of an entire game (like "Which team had the most wins in 2022?" or 
"How many wins did the NYY have?"), you must:
Identify the **final pitch** for each game by selecting the rows with the maximum 'at_bat_number' and then the row with the max `pitch_number`, grouped by `game_pk`. That final pitch row contains the game’s final scoreboard:
   - `post_home_score` (the home team’s final runs)
   - `post_away_score` (the away team’s final runs)

You will receive a user message describing what data they want from this `statcast_pitches` table in plain text. It is your job to translate the user request into a valid Postgres SQL query on this table.
Return only the valid SELECT query that accomplishes that. 
No other text or formatting is needed. 
""" 

async def main():
    # 1) Init OpenAI async client
    client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    # 2) Create DB engine for SQL queries
    engine = await get_async_engine("statcast")
    print("[Setup] Async engine created")

    # 3) Create the SQL Query Agent
    async_sql_agent = AsyncSQLQueryGeneratorAgent(
        client=client,
        engine=engine,
        openai_model="o1-mini",  # or "gpt-4o" or anything supported
        system_prompt=SQL_SYSTEM_PROMPT,
        output_format='csv'
    )
    print("[Setup] Async SQL Agent created")

    # 4) Create the VisualizationAgent 
    viz_asst_id = os.getenv("VIZ_ASSIST_ID")
    viz_agent = VisualizationAgent(
        async_sql_query_agent=async_sql_agent,
        openai_client=client,
        visualization_assistant_id=viz_asst_id
    )
    print("[Setup] VisualizationAgent created")

    analyst_asst_id = os.getenv("ANALYST_ASSIST_ID")

    # Create the AnalystAgent
    analyst_agent = AnalystAgent(
        openai_client=client,
        analyst_assistant_id=analyst_asst_id,
        query_agent=async_sql_agent,
        viz_agent=viz_agent
    )
    print("[Setup] AnalystAgent created")

    # Now let's do a simple REPL loop:
    while True:
        user_query = input("\nAsk a baseball question (or 'quit'): ").strip()
        if user_query.lower() in ["quit", "exit"]:
            break

        # 6) Create a new thread in the Assistants API
        new_thread = await client.beta.threads.create()
        thread_id = new_thread.id
        print(f"[REPL] Created new thread {thread_id}")

        # 7) Add user message
        user_msg = await client.beta.threads.messages.create(
            thread_id=thread_id,
            role="user",
            content=user_query
        )
        print("[REPL] User message added to thread.")

        # 8) Let the AnalystAgent handle the query
        await analyst_agent.handle_query(thread_id)

        # 9) Now let's fetch the last assistant message 
        #    to show the final answer
        msgs_page = await client.beta.threads.messages.list(thread_id=thread_id)
        msgs = msgs_page.data
        # The last message is presumably the final assistant's answer
        if not msgs:
            print("[REPL] No messages found in thread after run. Something is off.")
            continue

        # We'll find the last 'assistant' role message
        assistant_msg = None
        for m in reversed(msgs):
            if m.role == "assistant":
                assistant_msg = m
                break

        if assistant_msg:
            print("\n[AnalystAgent's final answer]:")
            for block in assistant_msg.content:
                if block.type == "text":
                    print(block.text.value)
                elif block.type == "image_file":
                    print(f"[Image attached: file_id={block.image_file.file_id}]")
        else:
            print("[REPL] No final assistant message was posted. Possibly no answer?")

asyncio.run(main())




