"""
SQL Query Generator Agent
-------------------------

This module defines a class `SQLQueryGeneratorAgent` that:
1. Accepts a list of natural-language query descriptions.
2. Uses the OpenAI Chat Completions API to generate SQL statements for each query.
3. Executes those SQL queries via a local `db_query()` function.
4. Returns the results.

Note:
 - You will need to install `openai` (pip install openai).
 - You should already have your environment variable `OPENAI_API_KEY` set,
   or use another method to provide credentials to the OpenAI client.
 - The `db_query` function is assumed to be your custom read-only DB query function.
   We'll import it from `db_query_function.py` (adjust the import as needed).
 - You must provide your own system prompt describing the database schema, so
   the model can accurately generate SQL for your Statcast data.
"""

import os
import openai
from typing import List, Dict, Any

# Adjust this import to your actual module/file structure
from utils.db_query import execute_sql_query

# Example: Provide a global system prompt describing your schema
SYSTEM_PROMPT = """
You are a helpful SQL generation assistant with expert knowledge of MLB Baseball. You have access to a Postgres table named `statcast_pitches` 
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
 - Short result code: B=ball, S=strike, X=in play. When type is 'B' or 'S' the "event" field will be NULL

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

fielder_3 (FLOAT)
 - MLB Player Id for 1B.

fielder_4 (FLOAT)
 - MLB Player Id for 2B.

fielder_5 (FLOAT)
 - MLB Player Id for 3B.

fielder_6 (FLOAT)
 - MLB Player Id for SS.

fielder_7 (FLOAT)
 - MLB Player Id for LF.

fielder_8 (FLOAT)
 - MLB Player Id for CF.

fielder_9 (FLOAT)
 - MLB Player Id for RF.

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
 - Change in Run Expectancy from pre to post pitch.

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
3) Only output the SQL statement, nothing else (no English explanation). 
4) If needed, you may use WHERE, GROUP BY, ORDER BY, LIMIT, etc., but no INSERT/UPDATE/DELETE.
5) Only use the column names specified above in your statements.
6) Missing values are present in the table and are represented as null, so **exclude** any rows in your query that have nulls in columns used for filters or aggregates 
   (for example, if you do an AVG on release_speed, add a WHERE release_speed IS NOT NULL).

When a question refers to the outcome of an entire game (like "Which team had the most wins in 2022?" or 
"How many wins did the NYY have?"), you must:
Identify the **final pitch** for each game by selecting the rows with the maximum 'at_bat_number' and then the row with the max `pitch_number`, grouped by `game_pk`. That final pitch row contains the game’s final scoreboard:
   - `post_home_score` (the home team’s final runs)
   - `post_away_score` (the away team’s final runs)

You will receive a user message describing what data they want from this `statcast_pitches` table in plain text. It is your job to translate the user request into a valid Postgres SQL query on this table.
Return only the valid SELECT query that accomplishes that. 
No other text or formatting is needed. 
""" 


class SQLQueryGeneratorAgent:
    """
    SQLQueryGeneratorAgent uses the OpenAI Chat Completions API (gpt-3.5/4, etc.)
    to convert natural-language queries into valid SQL statements, then executes them.
    """

    def __init__(self, client, db_conn, model_name: str = "gpt-4o", temperature: float = 0.0):
        """
        :param client: OpenAI Client Object
        :param db_conn: connection object to database
        :param model_name: e.g., "gpt-3.5-turbo" or "gpt-4"
        :param temperature: controls randomness of the output
        """
        self.client = client
        self.model_name = model_name
        self.temperature = temperature
        self.db_conn = db_conn

    def generate_and_run_queries(self, query_descriptions: List[str]) -> List[Dict[str, Any]]:
        """
        For each item in `query_descriptions`, call the Chat Completions API to generate SQL.
        Then run each SQL query using the local `execute_sql_query` function.
        
        :param query_descriptions: a list of user question strings.
        :return: a list of dictionaries with query results, one per item in `query_descriptions`.
        """
        results = []

        for desc in query_descriptions:
            sql = self._generate_sql(desc)
            # You could do more validation here, e.g. ensure only SELECT is present
            data = self._run_sql(sql)
            results.append({
                "query_description": desc,
                "generated_sql": sql,
                "data": data
            })

        return results

    def _generate_sql(self, description: str) -> str:
        """
        Calls the Chat Completions API with a system prompt describing the schema
        and a user message describing the query.
        Returns the SQL statement string (we expect a single SELECT).
        """

        if self.model_name == "o1-mini":

            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": f" {SYSTEM_PROMPT.strip()}   Generate a read-only SQL query for: '{description}'"
                        },
                    ],
                }
            ]

            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=messages
            )
        
        else:

            messages = [
                {"role": "system", "content": SYSTEM_PROMPT.strip()},
                {
                    "role": "user",
                    "content": f"Generate a read-only SQL query for: '{description}'"
                }
            ]


            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=messages,
                temperature=self.temperature
            )

        # Typically the response is in response.choices[0].message.content
        # We assume the model returns only the SQL code in text form
        sql_code = response.choices[0].message.content.strip()
   

        # Optional: You might parse out code fences if model includes them
        # e.g. if sql_code starts with ```sql ...
        if sql_code.startswith("```"):
            # strip triple-backticks and possible "sql" label
            sql_code = sql_code.strip("```").replace("sql\n", "").replace("sql", "")

        return sql_code

    def _run_sql(self, sql: str, output_format: str = "json") -> Any:
        """
        Executes the generated SQL using our local db_query() function.
        By default, returns the result as JSON (a string of JSON array),
        or you can adapt to return CSV, etc.
        """
        try:
            result_str = execute_sql_query(self.db_conn, sql, output_format=output_format)
            # If output_format == "json", result_str is a JSON string
            # Convert to Python data if you want
            # e.g. for a Python dict/list:
            # import json
            # data = json.loads(result_str)
            return result_str

        except Exception as e:
            # Logging, re-raising, or handle the error
            raise RuntimeError(f"SQL execution failed: {e}") from e


