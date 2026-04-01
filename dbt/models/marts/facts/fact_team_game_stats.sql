-- Fact model for team game statistics.
-- grain: one row per team per game (game_id + team_id)

with player_game_base as (
    select
        game_id,
        team_id,
        game_date,
        season_id,
        is_win,
        is_home,
        points,
        minutes,
        field_goals_made,
        field_goals_attempted,
        three_pointers_made,
        free_throws_made,
        total_rebounds,
        assists,
        steals,
        blocks,
        turnovers
    from {{ ref('int_player_game_enriched') }}
),

team_game_aggregates as (
    select
        concat(game_id, '_', team_id) as team_game_key,
        game_id,
        team_id,
        game_date,
        season_id,
        sum(points) as team_points,
        sum(minutes) as team_minutes,
        sum(field_goals_made) as field_goals_made,
        sum(field_goals_attempted) as field_goals_attempted,
        safe_divide(sum(field_goals_made), nullif(sum(field_goals_attempted), 0)) as field_goal_percentage,
        sum(three_pointers_made) as three_pointers_made,
        sum(free_throws_made) as free_throws_made,
        sum(total_rebounds) as total_rebounds,
        sum(assists) as assists,
        sum(steals) as steals,
        sum(blocks) as blocks,
        sum(turnovers) as turnovers,
        max(is_win) as is_win,
        max(is_home) as is_home,
        safe_divide(sum(points), nullif(sum(minutes), 0)) as points_per_minute
    from player_game_base
    group by
        game_id,
        team_id,
        game_date,
        season_id
)

select
    team_game_key,
    game_id,
    team_id,
    game_date,
    season_id,
    team_points,
    team_minutes,
    field_goals_made,
    field_goals_attempted,
    field_goal_percentage,
    three_pointers_made,
    free_throws_made,
    total_rebounds,
    assists,
    steals,
    blocks,
    turnovers,
    is_win,
    is_home,
    points_per_minute
from team_game_aggregates
