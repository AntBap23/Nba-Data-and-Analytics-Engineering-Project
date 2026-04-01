-- Intermediate model for enriched player game data.

with base as (
    select * from {{ ref('stg_player_game_logs') }}
),

final as (
    select 
        season_id,
        player_id,
        player_name,
        team_id,
        team_abbreviation,
        team_name,
        game_id,
        game_date,
        matchup,
        win_loss,
        minutes,
        points,
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
        season_label,

        case when win_loss = 'W' then 1 else 0 end as is_win,

        case when matchup like '%@%' then 1 else 0 end as is_away,
        case when matchup like '%@%' then 0 else 1 end as is_home,

        safe_divide(points, nullif(minutes, 0)) as points_per_minute

    from base
)

select * from final