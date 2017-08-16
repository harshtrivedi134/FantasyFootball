#!/usr/bin/python
import nflgame
import csv

#build the sets of key fields to retrieve and store
game_keys = [
"fumbles_forced",
"fumbles_lost",
"fumbles_notforced",
"fumbles_oob",
"fumbles_rec",
"fumbles_rec_yds",
"fumbles_tot",
"kicking_all_yds",
"kicking_fga",
"kicking_fgb",
"kicking_fgm",
"kicking_fgm_yds",
"kicking_fgmissed",
"kicking_fgmissed_yds",
"kicking_i20",
"kicking_tot",
"kicking_touchback",
"kicking_xpa",
"kicking_xpb",
"kicking_xpmade",
"kicking_xpmissed",
"kicking_yds",
"kickret_ret",
"kickret_touchback",
"kickret_yds",
"passing_att",
"passing_cmp",
"passing_cmp_air_yds",
"passing_incmp",
"passing_incmp_air_yds",
"passing_int",
"passing_sk",
"passing_sk_yds",
"passing_tds",
"passing_twopta",
"passing_twoptm",
"passing_twoptmissed",
"passing_yds",
"penalty",
"penalty_yds",
"punting_i20",
"punting_tot",
"punting_touchback",
"punting_yds",
"puntret_downed",
"puntret_fair",
"puntret_oob",
"puntret_tot",
"puntret_touchback",
"puntret_yds",
"receiving_rec",
"receiving_tar",
"receiving_tds",
"receiving_twopta",
"receiving_twoptm",
"receiving_twoptmissed",
"receiving_yac_yds",
"receiving_yds",
"rushing_att",
"rushing_tds",
"rushing_yds"
]

defense_keys = [
"first_downs",
"total_yds",
"passing_yds",
"rushing_yds",
"penalty_cnt",
"penalty_yds",
"turnovers",
"punt_cnt",
"punt_yds",
"punt_avg"
]

player_keys = [
"birthdate",
"college",
"first_name",
"gsis_name",
"height",
"last_name",
"name",
"number",
"position",
"status",
"team",
"uniform_number",
"weight",
"years_pro"
]

other_keys = [
"game_away",
"game_home",
"game_eid",
"game_loser",
"game_winner",
"game_score_home",
"game_score_away",
"game_score_home_q1",
"game_score_away_q1",
"game_score_home_q2",
"game_score_away_q2",
"game_score_home_q3",
"game_score_away_q3",
"game_score_home_q4",
"game_score_away_q4",
"game_score_home_q5",
"game_score_away_q5",
"game_week",
"game_year",
"game_time",
"game_wday",
"game_day",
"game_month",
"game_season_type",
"game_location",
"game_custom_id"
]



#build a collection of all the games in the API, due to library limitations, game collections will only represent games within a year, so we need a list of game collections
games_list = []
for year in range(2010,2017):
  games_list.append(nflgame.games(year))
games = [game for year in games_list for game in year]

#open the csv file, and build the header list from the sets of keys
with open('/mnt/c/Users/William/Documents/football/nfl_game_2016.csv', 'wb') as output:
  fieldnames = ["game_" + i for i in game_keys] + ["opponent_" + i for i in defense_keys] + ["player_" + i for i in player_keys] + other_keys
  csv_out = csv.DictWriter(output, fieldnames = fieldnames, delimiter = ',',quotechar = '"',restval = "", extrasaction = 'ignore')
  csv_out.writeheader()
  
  #loop through all the plays in the collection
  for game in games:
    for player in game.max_player_stats():
      out_dict = {}
          
      #include the stats for the composite play that the event was part of
      for key in game_keys:
        if key in player._stats:
          out_dict["game_" + key] = player._stats[key]
        elif key in player.__dict__:
          out_dict["game_" + key] = player.__dict__[key]
          
      #lookup the player and include the player's data
      for key in player_keys:
        if player.player is not None:
          if key in player.player.__dict__:
            out_dict["player_" + key] = player.player.__dict__[key]
            
      #include misc play, drive, and game fields
      out_dict["game_away"] = game.away
      out_dict["game_home"] = game.home
      out_dict["game_eid"] = game.eid
      out_dict["game_loser"] = game.loser
      out_dict["game_winner"] = game.winner
      out_dict["game_nice_score"] = game.nice_score()
      out_dict["game_score_home"] = game.score_home
      out_dict["game_score_away"] = game.score_away
      out_dict["game_score_home_q1"] = game.score_home_q1
      out_dict["game_score_away_q1"] = game.score_away_q1
      out_dict["game_score_home_q2"] = game.score_home_q2
      out_dict["game_score_away_q2"] = game.score_away_q2
      out_dict["game_score_home_q3"] = game.score_home_q3
      out_dict["game_score_away_q3"] = game.score_away_q3
      out_dict["game_score_home_q4"] = game.score_home_q4
      out_dict["game_score_away_q4"] = game.score_away_q4
      out_dict["game_score_home_q5"] = game.score_home_q5
      out_dict["game_score_away_q5"] = game.score_away_q5
      out_dict["game_week"] = game.schedule['week']
      out_dict["game_year"] = game.schedule['year']
      out_dict["game_time"] = game.schedule['time']
      out_dict["game_wday"] = game.schedule['wday']
      out_dict["game_day"] = game.schedule['day']
      out_dict["game_month"] = game.schedule['month']
      out_dict["game_season_type"] = game.schedule['season_type']
      out_dict["game_location"] = game.schedule['home']
      out_dict["game_custom_id"] = str(game.schedule['year'] % 1000) + ("%02d" % game.schedule['week']) + game.home + game.away
          
      csv_out.writerow(out_dict)

    out_dict = {}
    out_dict["player_name"] = game.away + "_defense"
    out_dict["player_position"] = "DEF"
    out_dict["player_team"] = game.away
    for key in defense_keys:
      out_dict["opponent_" + key] = game.stats_home.__dict__[key]

    out_dict["game_away"] = game.away
    out_dict["game_home"] = game.home
    out_dict["game_eid"] = game.eid
    out_dict["game_loser"] = game.loser
    out_dict["game_winner"] = game.winner
    out_dict["game_nice_score"] = game.nice_score()
    out_dict["game_score_home"] = game.score_home
    out_dict["game_score_away"] = game.score_away
    out_dict["game_score_home_q1"] = game.score_home_q1
    out_dict["game_score_away_q1"] = game.score_away_q1
    out_dict["game_score_home_q2"] = game.score_home_q2
    out_dict["game_score_away_q2"] = game.score_away_q2
    out_dict["game_score_home_q3"] = game.score_home_q3
    out_dict["game_score_away_q3"] = game.score_away_q3
    out_dict["game_score_home_q4"] = game.score_home_q4
    out_dict["game_score_away_q4"] = game.score_away_q4
    out_dict["game_score_home_q5"] = game.score_home_q5
    out_dict["game_score_away_q5"] = game.score_away_q5
    out_dict["game_week"] = game.schedule['week']
    out_dict["game_year"] = game.schedule['year']
    out_dict["game_time"] = game.schedule['time']
    out_dict["game_wday"] = game.schedule['wday']
    out_dict["game_day"] = game.schedule['day']
    out_dict["game_month"] = game.schedule['month']
    out_dict["game_season_type"] = game.schedule['season_type']
    out_dict["game_location"] = game.schedule['home']
    out_dict["game_custom_id"] = str(game.schedule['year'] % 1000) + ("%02d" % game.schedule['week']) + game.home + game.away
  
    csv_out.writerow(out_dict)

    
    out_dict = {}    
    out_dict["player_name"] = game.home + "_defense"
    out_dict["player_position"] = "DEF"
    out_dict["player_team"] = game.home
    for key in defense_keys:
      out_dict["opponent_" + key] = game.stats_away.__dict__[key]
      
    out_dict["game_away"] = game.away
    out_dict["game_home"] = game.home
    out_dict["game_eid"] = game.eid
    out_dict["game_loser"] = game.loser
    out_dict["game_winner"] = game.winner
    out_dict["game_nice_score"] = game.nice_score()
    out_dict["game_score_home"] = game.score_home
    out_dict["game_score_away"] = game.score_away
    out_dict["game_score_home_q1"] = game.score_home_q1
    out_dict["game_score_away_q1"] = game.score_away_q1
    out_dict["game_score_home_q2"] = game.score_home_q2
    out_dict["game_score_away_q2"] = game.score_away_q2
    out_dict["game_score_home_q3"] = game.score_home_q3
    out_dict["game_score_away_q3"] = game.score_away_q3
    out_dict["game_score_home_q4"] = game.score_home_q4
    out_dict["game_score_away_q4"] = game.score_away_q4
    out_dict["game_score_home_q5"] = game.score_home_q5
    out_dict["game_score_away_q5"] = game.score_away_q5
    out_dict["game_week"] = game.schedule['week']
    out_dict["game_year"] = game.schedule['year']
    out_dict["game_time"] = game.schedule['time']
    out_dict["game_wday"] = game.schedule['wday']
    out_dict["game_day"] = game.schedule['day']
    out_dict["game_month"] = game.schedule['month']
    out_dict["game_season_type"] = game.schedule['season_type']
    out_dict["game_location"] = game.schedule['home']
    out_dict["game_custom_id"] = str(game.schedule['year'] % 1000) + ("%02d" % game.schedule['week']) + game.home + game.away
  
    csv_out.writerow(out_dict)
        