def to_float(val):
    try:
        if val is None:
            return None
        s = str(val).strip().replace(',', '.')
        return float(s)
    except Exception:
        return None
 
# Lista completa das colunas desejadas (organizada na vertical)
cols = [
        "name",
        "starting_at",
        "result_info",
        "league_name",
        "league_country_id",
        "season_id",
        "home_team",
        "away_team",
        "home_goals",
        "away_goals",
        "home_position",
        "away_position",
        "home_corners",
        "away_corners",
        "home_shots-off-target",
        "away_shots-off-target",
        "home_shots-total",
        "away_shots-total",
        "home_attacks",
        "away_attacks",
        "home_dangerous-attacks",
        "away_dangerous-attacks",
        "home_ball-possession",
        "away_ball-possession",
        "home_penalties",
        "away_penalties",
        "home_shots-insidebox",
        "away_shots-insidebox",
        "home_shots-outsidebox",
        "away_shots-outsidebox",
        "home_offsides",
        "away_offsides",
        "home_goals-kicks",
        "away_goals-kicks",
        "home_goal-attempts",
        "away_goal-attempts",
        "home_fouls",
        "away_fouls",
        "home_saves",
        "away_saves",
        "home_yellowcards",
        "away_yellowcards",
        "odds_pinnacle_home",
        "odds_pinnacle_draw",
        "odds_pinnacle_away",
        "odds_btts_pinnacle_no",
        "odds_btts_pinnacle_yes",
        "odds_over_pinnacle_2",
        "odds_under_pinnacle_2",
        "odds_over_pinnacle_2_25",
        "odds_under_pinnacle_2_25",
        "odds_over_pinnacle_2_5",
        "odds_under_pinnacle_2_5",
        "odds_bet365_home",
        "odds_bet365_draw",
        "odds_bet365_away",
        "odds_btts_bet365_no",
        "odds_btts_bet365_yes",
        "odds_over_bet365_2_0",
        "odds_under_bet365_2_0",
        "odds_over_bet365_2_0, 2_5",
        "odds_under_bet365_2_0, 2_5",
        "odds_over_bet365_2_5",
        "odds_under_bet365_2_5",

]

numeric_cols = [
    "home_goals",
    "away_goals",
    "home_corners",
    "away_corners",
    "home_shots-off-target",
    "away_shots-off-target",
    "home_shots-total",
    "away_shots-total",
    "home_attacks",
    "away_attacks",
    "home_dangerous-attacks",
    "away_dangerous-attacks",
    "home_ball-possession",
    "away_ball-possession",
    "home_penalties",
    "away_penalties",
    "home_shots-insidebox",
    "away_shots-insidebox",
    "home_shots-outsidebox",
    "away_shots-outsidebox",
    "home_offsides",
    "away_offsides",
    "home_goals-kicks",
    "away_goals-kicks",
    "home_goal-attempts",
    "away_goal-attempts",
    "home_fouls",
    "away_fouls",
    "home_saves",
    "away_saves",
    "home_yellowcards",
    "away_yellowcards",
    "prob_home_pinacle",
    "prob_draw_pinacle",
    "prob_away_pinacle",
    "prob_btts_pinacle",
    "prob_no_btts_pinacle",
    "prob_over_pinnacle_2",
    "prob_under_pinnacle_2",
    "prob_over_pinnacle_2_25",
    "prob_under_pinnacle_2_25",
    "prob_over_pinnacle_2_5",
    "prob_under_pinnacle_2_5",
    "prob_bet365_home",
    "prob_bet365_draw",
    "prob_bet365_away",
    "prob_btts_bet365_no",
    "prob_btts_bet365_yes",
    "prob_over_bet365_2_0",
    "prob_under_bet365_2_0",
    "prob_over_bet365_2_0, 2_5",
    "prob_under_bet365_2_0, 2_5",
    "prob_over_bet365_2_5",
    "prob_under_bet365_2_5",
]
#....................................................................

ligas_2025 = [
    (25184, "Brasileirao Betano Serie A", "2025"),
    (24962, "EUA MLS", "2025"),
    (25583, "England Premier League", "2025-2026"),
    (25651, "France Ligue 1", "2025-2026"),
    (25652, "Germany Bundesliga 2", "2025-2026"),
    (25646, "Germany Bundesliga", "2025-2026"),
    (25745, "Liga Portugal", "2025-2026"),
    (25539, "Mexico Liga MX", "2025-2026"),
    (25690, "Portugal LigaPro", "2025-2026"),
    (25659, "Spain La Liga", "2025-2026"),
    (25673, "Spain La Liga 2", "2025-2026"),
    (25597, "Netherlands Eredivisie", "2025-2026"),
    (25044, "K League 1", "2025"),
    (25065, "Colombia Liga BetPlay", "2025"),
    (24946, "Japan J League", "2025"),
    (26276, "Saudi Arabia Liga Profissional", "2025"),
    (24969, "Argentina Liga Profissional de Futebol", "2025"),
    (25600, "Belgium Liga Pro", "2025-2026"),
    (25599, "Russia Premier League", "2025-2026"),
    (25107, "Uruguay Primera Division", "2025"),
    (25533, "Italy Serie A", "2025-2026"),
    (25682, "Turkey Super Lig", "2025-2026"),
    (25759, "Greece Super Liga", "2025-2026"),
    (25536, "Denmark Superliga", "2025-2026"),

]

liga_ids_2025 = [
25184, 24962, 25583, 25651, 25652, 25646, 25745, 25539, 25690, 25659,
25673, 25597, 25044, 25065, 24946, 26276, 24969, 25600, 25599, 25107,
25533, 25682, 25759, 25536
]
#.....................................................................................

ligas_2024 = [
    (23265, "Brasileirao Betano Serie A", "2024"),
    (22974, "EUA MLS", "2024"),
    (23091, "K League 1", "2024"),
    (23000, "Colombia Liga BetPlay", "2024"),
    (23002, "Japan J League", "2024"),
    (23171, "Uruguay Primera Division", "2024"),
    (23970, "Saudi Arabia Liga Profissional", "2024"),
    (23024, "Argentina Liga Profissional de Futebol", "2024"),    
    (23614, "England Premier League", "2024-2025"),
    (23643, "France Ligue 1", "2024-2025"),
    (23745, "Germany Bundesliga 2", "2024-2025"),
    (23744, "Germany Bundesliga", "2024-2025"),
    (23793, "Liga Portugal", "2024-2025"),
    (23586, "Mexico Liga MX", "2024-2025"),
    (23792, "Portugal LigaPro", "2024-2025"),
    (23621, "Spain La Liga", "2024-2025"),
    (23676, "Spain La Liga 2", "2024-2025"),
    (23628, "Netherlands Eredivisie", "2024-2025"),
    (23589, "Belgium Liga Pro", "2024-2025"),
    (23642, "Russia Premier League", "2024-2025"),
    (23746, "Italy Serie A", "2024-2025"),
    (23589, "Turkey Super Lig", "2024-2025"),
    (23923, "Greece Super Liga", "2024-2025"),
    (23584, "Denmark Superliga", "2024-2025"),

]

liga_ids_2024 = [
23265 , 22974, 23091 , 23000 , 23002, 23171, 23970, 23024 , 23614, 23643,
23745 , 23744 , 23793 , 23586 , 23792 , 23621 , 23676 , 23628 , 23589 , 23642,
23746 , 23589 , 23923 , 23584
]

#.....................................................................................


ligas_2023 = [
    (21207, "Brasileirao Betano Serie A", "2023"),
    (20901, "EUA MLS", "2023"),
    (21100, "K League 1", "2023"),
    (21091, "Colombia Liga BetPlay", "2023"),
    (21035, "Japan J League", "2023"),
    (21140, "Uruguay Primera Division", "2023"),
    (22031, "Saudi Arabia Liga Profissional", "2023"),    
    (20873, "Argentina Liga Profissional de Futebol", "2023"), 
    (21646, "England Premier League", "2023-2024"),
    (21779, "France Ligue 1", "2023-2024"),
    (21796, "Germany Bundesliga 2", "2023-2024"),
    (21795, "Germany Bundesliga", "2023-2024"),
    (21825, "Liga Portugal", "2023-2024"),
    (21623, "Mexico Liga MX", "2023-2024"),
    (21824, "Portugal LigaPro", "2023-2024"),
    (21694, "Spain La Liga", "2023-2024"),
    (21739, "Spain La Liga 2", "2023-2024"),
    (21730, "Netherlands Eredivisie", "2023-2024"),
    (21693, "Belgium Liga Pro", "2023-2024"),
    (21713, "Russia Premier League", "2023-2024"),
    (21818, "Italy Serie A", "2023-2024"),
    (22057, "Turkey Super Lig", "2023-2024"),
    (22043, "Greece Super Liga", "2023-2024"),
    (21644, "Denmark Superliga", "2023-2024"),

]

liga_ids_2023 = [
20873, 20901, 21035, 21091, 21100, 21140, 21207,21623, 21644, 21646,
21693,21694, 21713, 21730, 21739, 21779, 21795, 21796, 21818, 21824,
21825, 22031, 22043, 22057
]

#.....................................................................................
ligas_2022 = [
    (19434, "Brasileirao Betano Serie A", "2022"),
    (19137, "EUA MLS", "2022"),
    (19370, "K League 1", "2022"),
    (19326, "Colombia Liga BetPlay", "2022"),
    (19312, "Japan J League", "2022"),
    (19421, "Uruguay Primera Division", "2022"),
    (20310, "Saudi Arabia Liga Profissional", "2022"),
    (19353, "Argentina Liga Profissional de Futebol", "2022"),    
    (19734, "England Premier League", "2022-2023"),
    (19745, "France Ligue 1", "2022-2023"),
    (19743, "Germany Bundesliga 2", "2022-2023"),
    (19744, "Germany Bundesliga", "2022-2023"),
    (19896, "Liga Portugal", "2022-2023"),
    (19684, "Mexico Liga MX", "2022-2023"),
    (19895, "Portugal LigaPro", "2022-2023"),
    (19799, "Spain La Liga", "2022-2023"),
    (19800, "Spain La Liga 2", "2022-2023"),
    (19726, "Netherlands Eredivisie", "2022-2023"),
    (19785, "Belgium Liga Pro", "2022-2023"),
    (19781, "Russia Premier League", "2022-2023"),
    (19806, "Italy Serie A", "2022-2023"),
    (19900, "Turkey Super Lig", "2022-2023"),
    (20206, "Greece Super Liga", "2022-2023"),
    (19686, "Denmark Superliga", "2022-2023"),

]

liga_ids_2022 = [
    19137, 19312, 19326, 19353, 19370, 19421, 19434,19684, 19686, 19726, 
    19734, 19743, 19744, 19745, 19781, 19785, 19799, 19800, 19806,19895, 
    19896, 19900, 20206, 20310

]

#.....................................................................................


"""
colunas renomeadas como ficaram na noaquivo tratado e como eram no arquivo original extraido da api 

"Brasileirao Betano Serie A": "Brasileirão Betano Serie A",
"EUA MLS": "Liga Principal de Futebol",
"England Premier League":"Premier League",
"France Ligue 1":"Liga 1",
"Germany Bundesliga 2":"2. Bundesliga",
"Germany Bundesliga":"Bundesliga",
"Liga Portugal" : "Liga Portugal",
"Mexico Liga MX": "Liga MX",
"Portugal LigaPro": "LigaPro",
"Spain La Liga": "La Liga",
"Spain La Liga 2": "La Liga 2",
"Netherlands Eredivisie":"Eredivisie",
"K League 1": "K League 1",
"Colombia Liga BetPlay": "Liga BetPlay",
"Japan J League": "Liga J",
"Saudi Arabia Liga Profissional": "Liga Profissional",
"Argentina Liga Profissional de Futebol": "Liga Profissional de Futebol",
"Belgium Liga Pro": "Liga Profissional",
"Russia Premier League": "Primeira Liga",
"Uruguay Primera Division": "Primera Division",
"Italy Serie A": "Serie A",
"Turkey Super Lig": "Super Lig,
"Greece Super Liga": "Super Liga",
"Denmark Superliga": "Superliga,
"""
