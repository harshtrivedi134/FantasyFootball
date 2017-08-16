from pyspark.sql import HiveContext
from pyspark.mllib.feature import StandardScaler
from pyspark.mllib.feature import StandardScalerModel
from pyspark.mllib.stat import Statistics
import numpy
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.regression import LinearRegressionWithSGD
from pyspark.mllib.tree import RandomForest


#fields to use for prediction from each week, first entry should be value predicted (points)
fields = { 
  'qb': ['points', 'win', 'home', 'score', 'game_passing_tds', 'game_passing_twoptm', 'game_passing_int', 'game_fumbles_lost', 'game_passing_sk', 'game_passing_att', 'game_passing_cmp', 'game_passing_yds', 'temperature'],
  'wr': ['points', 'win', 'home', 'score', 'game_receiving_tds', 'game_receiving_rec', 'game_receiving_yds', 'game_receiving_tar', 'game_receiving_yac_yds', 'game_fumbles_lost', 'temperature'],
  'rb': ['points', 'win', 'home', 'score', 'game_rushing_tds', 'game_rushing_yds', 'game_fumbles_lost', 'temperature'],
  'te': ['points', 'win', 'home', 'score', 'game_receiving_tds', 'game_receiving_rec', 'game_receiving_yds', 'game_receiving_tar','game_rushing_tds', 'game_rushing_yds', 'game_fumbles_lost', 'temperature'],
  'k': ['points', 'win', 'home', 'score', 'game_kicking_fgm', 'game_kicking_fgm_yds', 'game_kicking_fgmissed', 'game_kicking_fgmissed_yds', 'game_kicking_xpmade','temperature'],
  'def': ['points', 'win', 'home', 'score', 'opponent_first_downs', 'opponent_total_yds', 'opponent_passing_yds', 'opponent_rushing_yds', 'opponent_turnovers', 'temperature']
  }

queries = {
  'qb': 'select * from qb left join qb_opp on qb.opp = qb_opp.qb_opp and qb.game_year = qb_opp.qb_game_year',
  'wr': 'select * from wr left join wr_opp on wr.opp = wr_opp.wr_opp and wr.game_year = wr_opp.wr_game_year',
  'rb': 'select * from rb left join rb_opp on rb.opp = rb_opp.rb_opp and rb.game_year = rb_opp.rb_game_year',
  'te': 'select * from te left join te_opp on te.opp = te_opp.te_opp and te.game_year = te_opp.te_game_year',
  'k': 'select * from k left join k_opp on k.opp = k_opp.k_opp and k.game_year = k_opp.k_game_year',
  'def': 'select * from def left join def_opp on def.opp = def_opp.def_opp and def.game_year = def_opp.def_game_year'  
  }


lin_predictions = {}
forrest_predictions = {}
hc = HiveContext(sc)


for position in ['qb', 'wr', 'rb', 'te','k', 'def']:
  
  #load data from Hive/Impala
  player_df = hc.sql(queries[position])
  
  player_list = player_df.filter(player_df['game_year'] == 2016).select('player_name').distinct().rdd.map(lambda i: i["player_name"]).collect()
  
  #week we are trying to predict
  predict_week = 14
  
  #how many historical weeks to use in the regression as features
  historical_weeks = 5 
  
  player_data = []
  player_predict_data = {}
  position_fields = fields[position]
  
  #For each player generate a dataset for training the model and to use for the weekly prediction
  for player in player_list:
    player_games = player_df.filter(player_df['player_name'] == player)
    total_points = player_games.groupBy('player_name').sum('points').collect()[0][1]
    game_data = {}
    
    #map the player data by season and week
    for game in player_games.collect():
      season = game['game_year']
      if season not in game_data:
        game_data[season] = {}
        game_data[season]['player'] = player
        game_data[season]['player_team'] = game['player_team']
      game_data[season][game['game_week']] = []
      
      #add each field and convert the data to floats
      for field in position_fields:
        try:
          game_data[season][game['game_week']].append((game[field] or 0.0) and float(game[field]))
        except:
          #treat dome or invalid temp as 70 degrees, all other invalid numbers as 0.0
          if field == 'temperature':
            game_data[season][game['game_week']].append(70.0)
          else:
            game_data[season][game['game_week']].append(0.0)
            
      #use total points and opponent adjustment for all positions
      game_data[season][game['game_week']].append(total_points)
      game_data[season][game['game_week']].append(game['opp_effect'])
      
    #turn the mapped data into a vector containing data for all the weeks the current weeks score will be based on
    for season in game_data:
      for week in range(16, historical_weeks+1, -1):
        if week not in game_data[season]:
          continue
        #add the player and the week's points then add previous week's data for building regression
        week_data = [game_data[season][week][0],game_data[season]['player']]
        for prev_week in range(week -1, week - historical_weeks, -1):
          if prev_week in game_data[season]:
            week_data.extend(game_data[season][prev_week])
          else:
            week_data.extend([0.0] * (len(position_fields) + 2))
        player_data.append(week_data)
        
      #build the vector for the current week to use for the prediction, need something in the point position
      #to keep vectors the same for scaling, then add the previous week's data
      player_predict_data[player] = [0]
      for week in range(predict_week-1, predict_week - historical_weeks, -1):
        if 2016 in game_data and week in game_data[2016]:
          player_predict_data[player].extend(game_data[2016][week])
        else:
          player_predict_data[player].extend([0.0] * (len(position_fields) + 2))
  
  #convert back to rdd and create features removing the player name for standardization and regression
  player_rdd = sc.parallelize(player_data)
  player_features = player_rdd.map(lambda i: [i[0]] + i[2:])
  
  #standardize the data for the regression
  scaler = StandardScaler(True, True)
  scaler_model = scaler.fit(player_features)
  scaled_player = scaler_model.transform(player_features)
  
  #calculate the values to use to destandardize the results since StandardScaler doesn't expose them
  player_summary = Statistics.colStats(player_features)
  mean = player_summary.mean()[0]
  std = numpy.sqrt(player_features.map(lambda i: i[0]).variance())
  
  scale_player_predict = {}
  for player in player_list:
    scale_player_predict[player] = scaler_model.transform(player_predict_data[player])
  
  #convert into an RDD of LabeledPoints used by the regression models.  They are tuples with the
  #first value the label (predicted value) and the second value a feature vector
  labeled_player = scaled_player.map(lambda i: LabeledPoint(i[0], i[1:]))
  
  
  #create the regression model from the set of LabeledPoints
  player_model = LinearRegressionWithSGD.train(labeled_player, iterations=20, step=0.1, intercept=False)
  
  #generate the predictions side by side with the actual labels for comparison
  prediction_comparison = labeled_player.map(lambda i: (i.label, player_model.predict(i.features)))
  
  #generate the predictions for the specified week
  lin_predictions[position] = {}
  for player in player_list:
    lin_predictions[position][player] = player_model.predict(scale_player_predict[player][1:]) * std + mean
    print(player + ': ' + str(lin_predictions[position][player]))
  print lin_predictions
  
  #build a random forrest model
  player_tree_model = RandomForest.trainRegressor(labeled_player, {}, 20, seed=1)
  
  #generate a prediction comparison dataset
  player_extracted_features = labeled_player.map(lambda i: i.features)
  player_extracted_labels = labeled_player.map(lambda i: i.label)
  tree_prediction_comparison = zip(player_extracted_labels.collect(),player_tree_model.predict(player_extracted_features).collect())
  
  #generate the random forrest predictions for the specified week
  forrest_predictions[position] = {}
  for player in player_list:
    forrest_predictions[position][player] = player_tree_model.predict(player_predict_data[player]) * std + mean
    print(player + ': ' + str(forrest_predictions[position][player]))
  print forrest_predictions