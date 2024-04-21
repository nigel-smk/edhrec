import pandas as pd

def _get_card_lists(commander_detail):
    if "cardlists" not in commander_detail["container"]["json_dict"]:
        return {}
    return {
        cardlist["tag"]: cardlist["cardviews"]
        for cardlist in commander_detail["container"]["json_dict"]["cardlists"]
    }
def _get_tribe(commander_detail):
    if len(commander_detail["container"]["breadcrumb"]) > 2:
        return next(iter(commander_detail["container"]["breadcrumb"][2].values()))
    else:
        return "base"
    
def extract_commander_details(commander_details_json):
    return [
        {
            "tribe": _get_tribe(commander_detail),
            **commander_detail["container"]["json_dict"]["card"],
            **_get_card_lists(commander_detail)
        }
        for commander_detail in commander_details_json
        if "container" in commander_detail
    ]

def flatten_cards(commander_df, card_types):
    flattened_cards_by_type = []
    for card_type in card_types:
        df = commander_df[['name', 'sanitized', 'num_decks', 'tribe', card_type]]
        df = df.explode(card_type)
        df.reset_index(drop=True, inplace=True)

        card_df = pd.json_normalize(df[card_type]).add_prefix('card_')
        df = pd.concat([df, card_df], axis=1)

        df = df.dropna(subset=['card_sanitized'])

        df['card_type'] = card_type
        df['card_inclusion'] = df["card_num_decks"] / df["card_potential_decks"]

        flattened_cards_by_type.append(df[[
            "name",
            "sanitized",
            "num_decks",
            "tribe",
            "card_name",
            "card_sanitized",
            "card_type",
            "card_num_decks",
            "card_potential_decks",
            "card_inclusion"
        ]])
    return pd.concat(flattened_cards_by_type)