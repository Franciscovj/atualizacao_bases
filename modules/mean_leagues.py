

def mean_leagues_odds(df):
    df['prob_home_pinacle'] = (1 / df['odds_pinnacle_home']).round(2)
    df['prob_draw_pinacle'] = (1 / df['odds_pinnacle_draw']).round(2)
    df['prob_away_pinacle'] = (1 / df['odds_pinnacle_away']).round(2)
    df['prob_btts_pinacle'] = (1 / df['odds_btts_pinnacle_yes']).round(2)
    df['prob_no_btts_pinacle'] = (1 / df['odds_btts_pinnacle_no']).round(2)
    df['prob_over_pinnacle_2'] = (1 / df['odds_over_pinnacle_2']).round(2)
    df['prob_under_pinnacle_2'] = (1 / df['odds_under_pinnacle_2']).round(2)
    df['prob_over_pinnacle_2_25'] = (1 / df['odds_over_pinnacle_2_25']).round(2)
    df['prob_under_pinnacle_2_25'] = (1 / df['odds_under_pinnacle_2_25']).round(2)
    df['prob_over_pinnacle_2_5'] = (1 / df['odds_over_pinnacle_2_5']).round(2)
    df['prob_under_pinnacle_2_5'] = (1 / df['odds_under_pinnacle_2_5']).round(2)
    df['prob_bet365_home'] = (1 / df['odds_bet365_home']).round(2)
    df['prob_bet365_draw'] = (1 / df['odds_bet365_draw']).round(2)
    df['prob_bet365_away'] = (1 / df['odds_bet365_away']).round(2)
    df['prob_btts_bet365_no'] = (1 / df['odds_btts_bet365_no']).round(2)
    df['prob_btts_bet365_yes'] = (1 / df['odds_btts_bet365_yes']).round(2)
    df['prob_over_bet365_2_0'] = (1 / df['odds_over_bet365_2_0']).round(2)
    df['prob_under_bet365_2_0'] = (1 / df['odds_under_bet365_2_0']).round(2)
    df['prob_over_bet365_2_0, 2_5'] = (1 / df['odds_over_bet365_2_0, 2_5']).round(2)
    df['prob_under_bet365_2_0, 2_5'] = (1 / df['odds_under_bet365_2_0, 2_5']).round(2)
    df['prob_over_bet365_2_5'] = (1 / df['odds_over_bet365_2_5']).round(2)
    df['prob_under_bet365_2_5'] = (1 / df['odds_under_bet365_2_5']).round(2)



# === 3. Função reutilizável para gerar médias ===
def gerar_media(df, tipo: str, group_col: str, numeric_cols: list):
    """
    Gera médias agrupadas por time (home ou away)
    e renomeia colunas com prefixos adequados.
    """
    df_media = df.groupby(group_col)[numeric_cols].mean().reset_index().round(2)

    new_columns = {}
    for col in numeric_cols:
        if col.startswith("home_"):
            if tipo == "home":
                new_columns[col] = col.replace("home_", "media_home_")
            else:
                new_columns[col] = col.replace("home_", "media_sofridos_away_")

        elif col.startswith("away_"):
            if tipo == "home":
                new_columns[col] = col.replace("away_", "media_sofridos_home_")
            else:
                new_columns[col] = col.replace("away_", "media_away_")

        elif col.startswith("odds_"):
            new_columns[col] = col.replace("odds_", f"media_odds_{tipo}_")

        elif col.startswith("prob_"):
            new_columns[col] = col.replace("prob_", f"media_prob_{tipo}_")

        else:
            new_columns[col] = f"media_{tipo}_" + col

    df_media.rename(columns=new_columns, inplace=True)
    df_media.rename(columns={group_col: "team"}, inplace=True)

    return df_media



