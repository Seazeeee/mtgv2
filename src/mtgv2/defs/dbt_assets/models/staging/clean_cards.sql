SELECT DISTINCT ON (oracle_id)
    oracle_id,
    name,
    type_line,
    oracle_text,
    mana_cost,
    cmc,
    power,
    toughness,
    loyalty,
    defense,
    keywords::TEXT[],
    reserved,
    content_warning,
    layout,
    hand_modifier,
    life_modifier
FROM {{ source('dagster', 'pull_scryfall_table') }}
WHERE oracle_id IS NOT NULL
ORDER BY oracle_id, released_at DESC