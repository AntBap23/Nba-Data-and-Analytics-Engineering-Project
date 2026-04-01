-- Fact model for player game statistics.
-- grain: player_id + game_id

with base as (
    select 
        season_id,
        game_date,
        player_id,
        game_id,
        team_id,

        concat(player_id, '_', game_id) as player_game_key,

        is_win,
        is_home,

        points,
        minutes,
        field_goals_made,
        field_goals_attempted,
        field_goal_percentage,
        three_pointers_made,
        free_throws_made,
        free_throw_percentage,
        offensive_rebounds,
        defensive_rebounds,
        total_rebounds,
        assists,
        steals,
        blocks,
        turnovers,
        personal_fouls,
        plus_minus,

        points_per_minute,

        safe_divide(field_goals_made, nullif(field_goals_attempted, 0)) as fg_efficiency,
        safe_divide(assists, nullif(turnovers, 0)) as ast_to_turnover_ratio

    from {{ ref('int_player_game_enriched') }}
)

select * from base