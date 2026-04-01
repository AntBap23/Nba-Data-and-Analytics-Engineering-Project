-- Staging model 


with source_2015 as (

    select 
    cast(SEASON_ID as int) as season_id,
    cast(PLAYER_ID as int) as player_id,
    cast(PLAYER_NAME as string) as player_name,
    cast(TEAM_ID as int) as team_id,
    cast(TEAM_ABBREVIATION as string) as team_abbreviation,
    cast(TEAM_NAME as string) as team_name,
    cast(GAME_ID as int) as game_id,
    cast(GAME_DATE as date) as game_date,
    cast(MATCHUP as string) as matchup,
    cast(WL as string) as win_loss,
    cast(MIN as int) as minutes,
    cast(PTS as int) as points,
    cast(FGM as int) as field_goals_made,
    cast(FGA as int) as field_goals_attempted,
    cast(FG_PCT as float) as field_goal_percentage,
    cast(FG3M as int) as three_pointers_made,
    cast(FTM as int) as free_throws_made,
    cast(FT_PCT as float) as free_throw_percentage,
    cast(OREB as int) as offensive_rebounds,
    cast(DREB as int) as defensive_rebounds,
    cast(REB as int) as total_rebounds,
    cast(AST as int) as assists,
    cast(STL as int) as steals,
    cast(BLK as int) as blocks,
    cast(TOV as int) as turnovers,
    cast(PF as int) as personal_fouls,
    cast(PLUS_MINUS as int) as plus_minus,
    cast(SEASON_LABEL as string) as season_label
    from {{ source('nba_raw', 'player_game_logs_2015_raw') }}

),

source_2016 as (
    select
    cast(SEASON_ID as int) as season_id,
    cast(PLAYER_ID as int) as player_id,
    cast(PLAYER_NAME as string) as player_name,
    cast(TEAM_ID as int) as team_id,
    cast(TEAM_ABBREVIATION as string) as team_abbreviation,
    cast(TEAM_NAME as string) as team_name,
    cast(GAME_ID as int) as game_id,
    cast(GAME_DATE as date) as game_date,
    cast(MATCHUP as string) as matchup,
    cast(WL as string) as win_loss,
    cast(MIN as int) as minutes,
    cast(PTS as int) as points,
    cast(FGM as int) as field_goals_made,
    cast(FGA as int) as field_goals_attempted,
    cast(FG_PCT as float) as field_goal_percentage,
    cast(FG3M as int) as three_pointers_made,
    cast(FTM as int) as free_throws_made,
    cast(FT_PCT as float) as free_throw_percentage,
    cast(OREB as int) as offensive_rebounds,
    cast(DREB as int) as defensive_rebounds,
    cast(REB as int) as total_rebounds,
    cast(AST as int) as assists,
    cast(STL as int) as steals,
    cast(BLK as int) as blocks,
    cast(TOV as int) as turnovers,
    cast(PF as int) as personal_fouls,
    cast(PLUS_MINUS as int) as plus_minus,
    cast(SEASON_LABEL as string) as season_label
    from {{ source('nba_raw', 'player_game_logs_2016_raw') }}

),

source_2017 as (
    select
    cast(SEASON_ID as int) as season_id,
    cast(PLAYER_ID as int) as player_id,
    cast(PLAYER_NAME as string) as player_name,
    cast(TEAM_ID as int) as team_id,
    cast(TEAM_ABBREVIATION as string) as team_abbreviation,
    cast(TEAM_NAME as string) as team_name,
    cast(GAME_ID as int) as game_id,
    cast(GAME_DATE as date) as game_date,
    cast(MATCHUP as string) as matchup,
    cast(WL as string) as win_loss,
    cast(MIN as int) as minutes,
    cast(PTS as int) as points,
    cast(FGM as int) as field_goals_made,
    cast(FGA as int) as field_goals_attempted,
    cast(FG_PCT as float) as field_goal_percentage,
    cast(FG3M as int) as three_pointers_made,
    cast(FTM as int) as free_throws_made,
    cast(FT_PCT as float) as free_throw_percentage,
    cast(OREB as int) as offensive_rebounds,
    cast(DREB as int) as defensive_rebounds,
    cast(REB as int) as total_rebounds,
    cast(AST as int) as assists,
    cast(STL as int) as steals,
    cast(BLK as int) as blocks,
    cast(TOV as int) as turnovers,
    cast(PF as int) as personal_fouls,
    cast(PLUS_MINUS as int) as plus_minus,
    cast(SEASON_LABEL as string) as season_label
    from {{ source('nba_raw', 'player_game_logs_2017_raw') }}

),

source_2018 as (
    select
    cast(SEASON_ID as int) as season_id,
    cast(PLAYER_ID as int) as player_id,
    cast(PLAYER_NAME as string) as player_name,
    cast(TEAM_ID as int) as team_id,
    cast(TEAM_ABBREVIATION as string) as team_abbreviation,
    cast(TEAM_NAME as string) as team_name,
    cast(GAME_ID as int) as game_id,
    cast(GAME_DATE as date) as game_date,
    cast(MATCHUP as string) as matchup,
    cast(WL as string) as win_loss,
    cast(MIN as int) as minutes,
    cast(PTS as int) as points,
    cast(FGM as int) as field_goals_made,
    cast(FGA as int) as field_goals_attempted,
    cast(FG_PCT as float) as field_goal_percentage,
    cast(FG3M as int) as three_pointers_made,
    cast(FTM as int) as free_throws_made,
    cast(FT_PCT as float) as free_throw_percentage,
    cast(OREB as int) as offensive_rebounds,
    cast(DREB as int) as defensive_rebounds,
    cast(REB as int) as total_rebounds,
    cast(AST as int) as assists,
    cast(STL as int) as steals,
    cast(BLK as int) as blocks,
    cast(TOV as int) as turnovers,
    cast(PF as int) as personal_fouls,
    cast(PLUS_MINUS as int) as plus_minus,
    cast(SEASON_LABEL as string) as season_label
    from {{ source('nba_raw', 'player_game_logs_2018_raw') }}

),

source_2019 as (
    select
    cast(SEASON_ID as int) as season_id,
    cast(PLAYER_ID as int) as player_id,
    cast(PLAYER_NAME as string) as player_name,
    cast(TEAM_ID as int) as team_id,
    cast(TEAM_ABBREVIATION as string) as team_abbreviation,
    cast(TEAM_NAME as string) as team_name,
    cast(GAME_ID as int) as game_id,
    cast(GAME_DATE as date) as game_date,
    cast(MATCHUP as string) as matchup,
    cast(WL as string) as win_loss,
    cast(MIN as int) as minutes,
    cast(PTS as int) as points,
    cast(FGM as int) as field_goals_made,
    cast(FGA as int) as field_goals_attempted,
    cast(FG_PCT as float) as field_goal_percentage,
    cast(FG3M as int) as three_pointers_made,
    cast(FTM as int) as free_throws_made,
    cast(FT_PCT as float) as free_throw_percentage,
    cast(OREB as int) as offensive_rebounds,
    cast(DREB as int) as defensive_rebounds,
    cast(REB as int) as total_rebounds,
    cast(AST as int) as assists,
    cast(STL as int) as steals,
    cast(BLK as int) as blocks,
    cast(TOV as int) as turnovers,
    cast(PF as int) as personal_fouls,
    cast(PLUS_MINUS as int) as plus_minus,
    cast(SEASON_LABEL as string) as season_label
    from {{ source('nba_raw', 'player_game_logs_2019_raw') }}

),

source_2020 as (
    select
    cast(SEASON_ID as int) as season_id,
    cast(PLAYER_ID as int) as player_id,
    cast(PLAYER_NAME as string) as player_name,
    cast(TEAM_ID as int) as team_id,
    cast(TEAM_ABBREVIATION as string) as team_abbreviation,
    cast(TEAM_NAME as string) as team_name,
    cast(GAME_ID as int) as game_id,
    cast(GAME_DATE as date) as game_date,
    cast(MATCHUP as string) as matchup,
    cast(WL as string) as win_loss,
    cast(MIN as int) as minutes,
    cast(PTS as int) as points,
    cast(FGM as int) as field_goals_made,
    cast(FGA as int) as field_goals_attempted,
    cast(FG_PCT as float) as field_goal_percentage,
    cast(FG3M as int) as three_pointers_made,
    cast(FTM as int) as free_throws_made,
    cast(FT_PCT as float) as free_throw_percentage,
    cast(OREB as int) as offensive_rebounds,
    cast(DREB as int) as defensive_rebounds,
    cast(REB as int) as total_rebounds,
    cast(AST as int) as assists,
    cast(STL as int) as steals,
    cast(BLK as int) as blocks,
    cast(TOV as int) as turnovers,
    cast(PF as int) as personal_fouls,
    cast(PLUS_MINUS as int) as plus_minus,
    cast(SEASON_LABEL as string) as season_label
    from {{ source('nba_raw', 'player_game_logs_2020_raw') }}

),

source_2021 as (
    select
    cast(SEASON_ID as int) as season_id,
    cast(PLAYER_ID as int) as player_id,
    cast(PLAYER_NAME as string) as player_name,
    cast(TEAM_ID as int) as team_id,
    cast(TEAM_ABBREVIATION as string) as team_abbreviation,
    cast(TEAM_NAME as string) as team_name,
    cast(GAME_ID as int) as game_id,
    cast(GAME_DATE as date) as game_date,
    cast(MATCHUP as string) as matchup,
    cast(WL as string) as win_loss,
    cast(MIN as int) as minutes,
    cast(PTS as int) as points,
    cast(FGM as int) as field_goals_made,
    cast(FGA as int) as field_goals_attempted,
    cast(FG_PCT as float) as field_goal_percentage,
    cast(FG3M as int) as three_pointers_made,
    cast(FTM as int) as free_throws_made,
    cast(FT_PCT as float) as free_throw_percentage,
    cast(OREB as int) as offensive_rebounds,
    cast(DREB as int) as defensive_rebounds,
    cast(REB as int) as total_rebounds,
    cast(AST as int) as assists,
    cast(STL as int) as steals,
    cast(BLK as int) as blocks,
    cast(TOV as int) as turnovers,
    cast(PF as int) as personal_fouls,
    cast(PLUS_MINUS as int) as plus_minus,
    cast(SEASON_LABEL as string) as season_label
    from {{ source('nba_raw', 'player_game_logs_2021_raw')}}
),

source_2022 as (
    select
    cast(SEASON_ID as int) as season_id,
    cast(PLAYER_ID as int) as player_id,
    cast(PLAYER_NAME as string) as player_name,
    cast(TEAM_ID as int) as team_id,
    cast(TEAM_ABBREVIATION as string) as team_abbreviation,
    cast(TEAM_NAME as string) as team_name,
    cast(GAME_ID as int) as game_id,
    cast(GAME_DATE as date) as game_date,
    cast(MATCHUP as string) as matchup,
    cast(WL as string) as win_loss,
    cast(MIN as int) as minutes,
    cast(PTS as int) as points,
    cast(FGM as int) as field_goals_made,
    cast(FGA as int) as field_goals_attempted,
    cast(FG_PCT as float) as field_goal_percentage,
    cast(FG3M as int) as three_pointers_made,
    cast(FTM as int) as free_throws_made,
    cast(FT_PCT as float) as free_throw_percentage,
    cast(OREB as int) as offensive_rebounds,
    cast(DREB as int) as defensive_rebounds,
    cast(REB as int) as total_rebounds,
    cast(AST as int) as assists,
    cast(STL as int) as steals,
    cast(BLK as int) as blocks,
    cast(TOV as int) as turnovers,
    cast(PF as int) as personal_fouls,
    cast(PLUS_MINUS as int) as plus_minus,
    cast(SEASON_LABEL as string) as season_label
    from {{ source('nba_raw', 'player_game_logs_2022_raw')}}
),

source_2023 as (
select
    cast(SEASON_ID as int) as season_id,
    cast(PLAYER_ID as int) as player_id,
    cast(PLAYER_NAME as string) as player_name,
    cast(TEAM_ID as int) as team_id,
    cast(TEAM_ABBREVIATION as string) as team_abbreviation,
    cast(TEAM_NAME as string) as team_name,
    cast(GAME_ID as int) as game_id,
    cast(GAME_DATE as date) as game_date,
    cast(MATCHUP as string) as matchup,
    cast(WL as string) as win_loss,
    cast(MIN as int) as minutes,
    cast(PTS as int) as points,
    cast(FGM as int) as field_goals_made,
    cast(FGA as int) as field_goals_attempted,
    cast(FG_PCT as float) as field_goal_percentage,
    cast(FG3M as int) as three_pointers_made,
    cast(FTM as int) as free_throws_made,
    cast(FT_PCT as float) as free_throw_percentage,
    cast(OREB as int) as offensive_rebounds,
    cast(DREB as int) as defensive_rebounds,
    cast(REB as int) as total_rebounds,
    cast(AST as int) as assists,
    cast(STL as int) as steals,
    cast(BLK as int) as blocks,
    cast(TOV as int) as turnovers,
    cast(PF as int) as personal_fouls,
    cast(PLUS_MINUS as int) as plus_minus,
    cast(SEASON_LABEL as string) as season_label
    from {{ source('nba_raw', 'player_game_logs_2023_raw')}}
),

source_2024 as (
    select
        cast(SEASON_ID as int) as season_id,
    cast(PLAYER_ID as int) as player_id,
    cast(PLAYER_NAME as string) as player_name,
    cast(TEAM_ID as int) as team_id,
    cast(TEAM_ABBREVIATION as string) as team_abbreviation,
    cast(TEAM_NAME as string) as team_name,
    cast(GAME_ID as int) as game_id,
    cast(GAME_DATE as date) as game_date,
    cast(MATCHUP as string) as matchup,
    cast(WL as string) as win_loss,
    cast(MIN as int) as minutes,
    cast(PTS as int) as points,
    cast(FGM as int) as field_goals_made,
    cast(FGA as int) as field_goals_attempted,
    cast(FG_PCT as float) as field_goal_percentage,
    cast(FG3M as int) as three_pointers_made,
    cast(FTM as int) as free_throws_made,
    cast(FT_PCT as float) as free_throw_percentage,
    cast(OREB as int) as offensive_rebounds,
    cast(DREB as int) as defensive_rebounds,
    cast(REB as int) as total_rebounds,
    cast(AST as int) as assists,
    cast(STL as int) as steals,
    cast(BLK as int) as blocks,
    cast(TOV as int) as turnovers,
    cast(PF as int) as personal_fouls,
    cast(PLUS_MINUS as int) as plus_minus,
    cast(SEASON_LABEL as string) as season_label
    from {{ source('nba_raw', 'player_game_logs_2024_raw')}}
),

combined as (
    select * from source_2015
    union all
    select * from source_2016
    union all
    select * from source_2017
    union all
    select * from source_2018
    union all
    select * from source_2019
    union all
    select * from source_2020
    union all
    select * from source_2021
    union all
    select * from source_2022
    union all
    select * from source_2023
    union all
    select * from source_2024
),

final as (
    select * 
    from combined
)

select * from final
