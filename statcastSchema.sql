CREATE TABLE statcast_pitches (

    pitch_type                       VARCHAR(30),
    game_date                        DATE,
    release_speed                    FLOAT,
    release_pos_x                    FLOAT,
    release_pos_z                    FLOAT,
    batter                           INT,
    pitcher                          INT,
    events                           VARCHAR(100),
    description                      VARCHAR(500),
    zone                             FLOAT,
    des                              VARCHAR(500),
    game_type                        VARCHAR(1),
    stand                            VARCHAR(1),
    p_throws                         VARCHAR(1),
    home_team                        VARCHAR(10),
    away_team                        VARCHAR(10),
    type                             VARCHAR(1),
    hit_location                     FLOAT,
    bb_type                          VARCHAR(30),
    balls                            INT,
    strikes                          INT,
    game_year                        INT,
    pfx_x                            FLOAT,
    pfx_z                            FLOAT,
    plate_x                          FLOAT,
    plate_z                          FLOAT,
    on_3b                            FLOAT,
    on_2b                            FLOAT,
    on_1b                            FLOAT,
    outs_when_up                     INT,
    inning                           INT,
    inning_topbot                    VARCHAR(3),
    hc_x                             FLOAT,
    hc_y                             FLOAT,
    fielder_2                        FLOAT,
    sv_id                            VARCHAR(50),
    vx0                              FLOAT,
    vy0                              FLOAT,
    vz0                              FLOAT,
    ax                               FLOAT,
    ay                               FLOAT,
    az                               FLOAT,
    sz_top                           FLOAT,
    sz_bot                           FLOAT,
    hit_distance_sc                  FLOAT,
    launch_speed                     FLOAT,
    launch_angle                     FLOAT,
    effective_speed                  FLOAT,
    release_spin_rate                FLOAT,
    release_extension                FLOAT,
    game_pk                          INT,
    fielder_3                        FLOAT,
    fielder_4                        FLOAT,
    fielder_5                        FLOAT,
    fielder_6                        FLOAT,
    fielder_7                        FLOAT,
    fielder_8                        FLOAT,
    fielder_9                        FLOAT,
    release_pos_y                    FLOAT,
    estimated_ba_using_speedangle    FLOAT,
    estimated_woba_using_speedangle  FLOAT,
    woba_value                       FLOAT,
    woba_denom                       FLOAT,
    babip_value                      FLOAT,
    iso_value                        FLOAT,
    launch_speed_angle               FLOAT,
    at_bat_number                    INT,
    pitch_number                     INT,
    pitch_name                       VARCHAR(50),
    home_score                       INT,
    away_score                       INT,
    bat_score                        INT,
    fld_score                        INT,
    post_away_score                  INT,
    post_home_score                  INT,
    post_bat_score                   INT,
    post_fld_score                   INT,
    if_fielding_alignment            VARCHAR(50),
    of_fielding_alignment            VARCHAR(50),
    spin_axis                        FLOAT,
    delta_home_win_exp               FLOAT,
    delta_run_exp                    FLOAT,
    bat_speed                        FLOAT,
    swing_length                     FLOAT,
    batter_name                      VARCHAR(100),
    pitcher_name                     VARCHAR(100)
);

