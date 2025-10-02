SELECT name, oracle_id 
FROM {{ source('public', 'scryfall_cards_raw') }}
