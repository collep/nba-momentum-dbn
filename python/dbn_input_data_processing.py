import pandas as pd
from python.pbp_event_mapping_config import events_config, event_columns_to_sum_teams, overall_events, engineered_features, additional_categorical_bins
import progressbar
import numpy as np
import os
def mapEvents(row):
    """
    Map predefined events to a single play-by-play row based on event definitions.

    Evaluates whether a given play-by-play row satisfies the conditions defined for each event in the global
    events_config list. These conditions can include  action type, sub-type, descriptors, qualifiers, shot results, etc.
    If a row meets all criteria for a given event, a new column with the event's name is added to the row and set to 1.

    Args:
    - row (pd.Series): A single row of play-by-play data

    Returns:
    - pd.Series: The original row with additional binary columns indicating whether each event
      condition in `events_config` was satisfied.
    """
    def checkEvent(row, event):
        # Check for actionType
        if event['actionType'] is not None and not any(at in event['actionType'] for at in [row['actionType']]):
            return False

        # Check for subType
        if event['subType'] is not None and not any(st in event['subType'] for st in [row['subType']]):
            return False

        # Check for exclusion of subType
        if event['exclude_subType'] is not None and any(est in event['exclude_subType'] for est in [row['subType']]):
            return False

        # Check for qualifiers (assuming row['qualifiers'] is a list)
        if event['qualifiers'] is not None and not set(event['qualifiers']).intersection(set(row['qualifiers'])):
            return False

        # Check for exclusion of qualifiers
        if event['exclude_qualifiers'] is not None and set(event['exclude_qualifiers']).intersection(
                set(row['qualifiers'])):
            return False

        # Check for descriptor
        if event['descriptor'] is not None and row['descriptor'] not in event['descriptor']:
            return False

        # Check for exclusion of descriptor
        if event['exclude_descriptor'] is not None and row['descriptor'] in event['exclude_descriptor']:
            return False

        # Check for shotResult
        if event['shotResult'] is not None and row['shotResult'] not in event['shotResult']:
            return False

        # Check for assisted
        if event['assisted'] is True and (pd.isna(row['assistPersonId']) or row['assistPersonId'] == ''):
            return False
        elif event['assisted'] is False and not (pd.isna(row['assistPersonId']) or row['assistPersonId'] == ''):
            return False

        if event['steal'] is True and (pd.isna(row['stealPersonId']) or row['stealPersonId'] == ''):
            return False

        if event['block'] is True and (pd.isna(row['blockPersonId']) or row['blockPersonId'] == ''):
            return False

        if event['new_offense_poss'] is True and (pd.isna(row['new_offense_poss']) or row['new_offense_poss'] == ''):
            return False

        return True

    for event in events_config:
        row[event['name']] = 1 if checkEvent(row, event) else 0
    return row

def sumTeamEventsWithinIntervals(gameData, time_window):
    """
        Sums binary event indicators and offensive possessions for each team and interval.

        Args:
        - gameData (pd.DataFrame): Play-by-play data for a single game that contain the mapped events as binary indicators
        - time_window (int): Length of each time interval in seconds (e.g., 120 for two-minute windows).

        Returns:
        - pd.DataFrame: Aggregated event counts per team per interval.
    """
    # Calculate time_window, window_start, and window_end all at once to reduce fragmentation
    time_window_column = ((gameData['eventClock'] - 1) // time_window) + 1
    window_start_column = (time_window_column - 1) * time_window
    window_end_column = time_window_column * time_window

    # Adjust time_window for the maximum event time
    max_event_time = gameData['eventClock'].max()
    if max_event_time % time_window == 0:
        time_window_column.loc[gameData['eventClock'] == max_event_time] = max_event_time // time_window

    home_team_id = gameData['HOME_TEAM_ID'].unique()[0]
    away_team_id = gameData['AWAY_TEAM_ID'].unique()[0]

    # Concatenate new columns to gameData
    gameData = pd.concat([gameData, time_window_column.rename('time_window'),
                           window_start_column.rename('window_start'),
                           window_end_column.rename('window_end')], axis=1)


    gameData = gameData.assign(
        time_window=np.where(gameData['time_window'] == 0, 1, gameData['time_window']),
        window_start=np.where(gameData['time_window'] == 0, 0, gameData['window_start']),
        window_end=np.where(gameData['time_window'] == 0, time_window, gameData['window_end'])
    )

    gameData['smaller_time_window'] = gameData['window_end'].apply(lambda x: 1 if x > max_event_time else 0)

    event_columns_to_sum = [col for col in event_columns_to_sum_teams if col != 'offense_poss']
    # Group by teamId and time_window, then sum the event columns
    groupedSum = gameData.groupby(['gameid', 'teamId', 'time_window'])[event_columns_to_sum].sum().reset_index()

    # Count the possesions (these are not binary indicators)
    possSum = gameData.groupby(['gameid', 'new_offense_poss', 'time_window'])['offense_poss'].sum().reset_index()
    possSum = possSum.rename(columns={"new_offense_poss": "teamId"})

    # Merge event and possesion counts
    sumsMerged = groupedSum.merge(possSum, how = 'left', left_on = ['gameid', 'teamId', 'time_window'],
                                    right_on = ['gameid', 'teamId', 'time_window'],suffixes=(False,'_drop'))


    window_times = gameData[['time_window', 'window_start', 'window_end','smaller_time_window']].drop_duplicates()

    # Add metadata for the intervals
    groupedSum = sumsMerged.merge(window_times, on='time_window', how='left')
    groupedSum['HOME_TEAM_ID'] = home_team_id
    groupedSum['AWAY_TEAM_ID'] = away_team_id

    return groupedSum

def sumOverallEventsWithinIntervals(gameData, time_window):
    """
        Sums overall (non-team-specific) event counts within fixed time intervals.

        Args:
        - gameData (pd.DataFrame):  Play-by-play data for a single game that contain the mapped events as binary indicators
        - time_window (int): Length of each time interval in seconds (e.g., 120 for two-minute windows).

        Returns:
        - pd.DataFrame: Aggregated counts of overall events for each interval.
    """
    time_window_column = ((gameData['eventClock'] - 1) // time_window) + 1

    max_event_time = gameData['eventClock'].max()
    if max_event_time % time_window == 0:
        time_window_column.loc[gameData['eventClock'] == max_event_time] = max_event_time // time_window

    gameData = pd.concat([gameData, time_window_column.rename('time_window')], axis=1)

    groupedSum = gameData.groupby(['time_window'])[overall_events].sum().reset_index()

    return groupedSum

def formatEventsWithTeams(eventSums):
    """
        Format and separate event counts by team (home vs. away) for each time interval.

        Args:
        - eventSums (pd.DataFrame): A DataFrame containing event counts per team per time window.
          Must include columns for 'gameid', 'teamId', 'HOME_TEAM_ID', 'AWAY_TEAM_ID'.

        Returns:
        - pd.DataFrame: A DataFrame with one row per game and time window, where each event count
          is split into separate columns for the home and away teams.
    """
    new_columns_home = {}
    new_columns_away = {}

    events = eventSums.columns.values.tolist()
    events.remove("gameid")
    events.remove("teamId")
    events.remove("time_window")
    events.remove("window_start")
    events.remove("window_end")
    events.remove('HOME_TEAM_ID')
    events.remove('AWAY_TEAM_ID')

    opposite_team_stats = ['Block','Steal']
    # Prepare the new columns outside the main DataFrame
    for event in events:
        home_col_name = f"{event}_home"
        away_col_name = f"{event}_away"

        if event in opposite_team_stats:
            continue
        new_columns_home[home_col_name] = eventSums.apply(
            lambda row: row[event] if row['teamId'] == row['HOME_TEAM_ID'] else 0, axis=1)
        new_columns_away[away_col_name] = eventSums.apply(
            lambda row: row[event] if row['teamId'] == row['AWAY_TEAM_ID'] else 0, axis=1)


    for opp_event in opposite_team_stats:
        home_col_name = f"{opp_event}_home"
        away_col_name = f"{opp_event}_away"

        new_columns_home[home_col_name] = eventSums.apply(
            lambda row: row[opp_event] if row['teamId'] == row['AWAY_TEAM_ID'] else 0, axis=1)
        new_columns_away[away_col_name] = eventSums.apply(
            lambda row: row[opp_event] if row['teamId'] == row['HOME_TEAM_ID'] else 0, axis=1)


    homeColumnsDf = pd.DataFrame(new_columns_home)
    awayColumnsDf = pd.DataFrame(new_columns_away)

    mergedDf = pd.concat([eventSums, homeColumnsDf, awayColumnsDf], axis=1)

    # Drop the original event columns and the teamId column as they are no longer needed
    mergedDf.drop(columns= events + ['teamId','HOME_TEAM_ID', 'AWAY_TEAM_ID',
                                      "window_start","window_end"], inplace=True)

    # Assuming 'time_window' and 'gameId' uniquely identify rows for aggregation
    # Aggregate rows to ensure one row per time period per game
    finalDf = mergedDf.groupby(['gameid', 'time_window']).sum().reset_index()

    return finalDf

def calculateEngineeredEvents(aggregatedGameData):
    """
    Compute engineered features from event counts.

    Creates new 'standardized' features by applying expressions to numerator and denominator inputs as defined in the
    engineered_features from pbp_event_mapping_config. Each new feature is calculated as a ratio of these expressions
    and added to the aggregated event-level DataFrame.

    Args:
    - aggregatedGameData (pd.DataFrame): A DataFrame containing summed event counts per team and time window.
      Must include all columns referenced in `engineered_features`.

    Returns:
    - pd.DataFrame: Original input with additional columns for each engineered feature.
    """
    new_columns = {}

    for stat in engineered_features:
        if stat['calc']==None:
            continue
        new_col = stat['name']
        num_expr, denom_expr = stat['calc']

        # Process numerator expression
        if isinstance(num_expr, tuple):
            if num_expr[0] == 'sum':
                num_val = aggregatedGameData[list(num_expr[1])].sum(axis=1)
            elif num_expr[0] == 'diff':
                num_val = aggregatedGameData[num_expr[1][0]] - aggregatedGameData[num_expr[1][1]]
        else:
            num_val = aggregatedGameData[num_expr]

        # Process denominator expression
        if isinstance(denom_expr, tuple):
            if denom_expr[0] == 'sum':
                denom_val = aggregatedGameData[list(denom_expr[1])].sum(axis=1)
            elif denom_expr[0] == 'diff':
                denom_val = aggregatedGameData[denom_expr[1][0]] - aggregatedGameData[denom_expr[1][1]]
        else:
            denom_val = aggregatedGameData[denom_expr]

        # Perform standardization
        new_columns[new_col] = num_val / denom_val.replace({0: None})

    engineeredFeatures = pd.DataFrame(new_columns, index=aggregatedGameData.index)
    engineeredFeatures = engineeredFeatures.fillna(0)

    aggregatedGameData = pd.concat([aggregatedGameData, engineeredFeatures], axis=1)

    return aggregatedGameData

def calculateTeamTaggedFeatures(aggregatedGameData):
    """
        Compute engineered features that require home and away team separation.

        Args:
        - aggregatedGameData (pd.DataFrame): A DataFrame containing event counts for home and away teams
          per time window.

        Modifies:
        - Adds the following columns to `aggregatedGameData`:
            - 'DReb_Rate_home', 'DReb_Rate_away'
            - 'OReb_Rate_home', 'OReb_Rate_away'
            - 'FastBreak_PerOppMiss_home', 'FastBreak_PerOppMiss_away'
            - 'Missed_Shots_home', 'Missed_Shots_away'
        - The original DataFrame is modified in place; no value is returned.
    """
    aggregatedGameData['DReb_Rate_home'] = aggregatedGameData['DefensiveRBD_home'] / (aggregatedGameData['DefensiveRBD_home']
                                            + aggregatedGameData['OffensiveRBD_away']).replace({0: None})
    aggregatedGameData['DReb_Rate_away'] = aggregatedGameData['DefensiveRBD_away'] / (aggregatedGameData['DefensiveRBD_away']
                                            + aggregatedGameData['OffensiveRBD_home']).replace({0: None})
    aggregatedGameData['OReb_Rate_home'] = aggregatedGameData['OffensiveRBD_home'] / (aggregatedGameData['DefensiveRBD_away']
                                            + aggregatedGameData['OffensiveRBD_home']).replace({0: None})
    aggregatedGameData['OReb_Rate_away'] = aggregatedGameData['OffensiveRBD_away'] / (aggregatedGameData['DefensiveRBD_home']
                                            + aggregatedGameData['OffensiveRBD_away']).replace({0: None})
    aggregatedGameData['FastBreak_PerOppMiss_away'] = aggregatedGameData['Shot_FstBrk_Attmpt_away'] / (aggregatedGameData['Shot_Attmpt_home']
                                            - aggregatedGameData['Shot_Make_home']).replace({0: None})
    aggregatedGameData['FastBreak_PerOppMiss_home'] = aggregatedGameData['Shot_FstBrk_Attmpt_home'] / (aggregatedGameData['Shot_Attmpt_away']
                                            - aggregatedGameData['Shot_Make_away']).replace({0: None})

    aggregatedGameData['Missed_Shots_home'] = aggregatedGameData['Shot_Attmpt_home'] - aggregatedGameData['Shot_Make_home']

    aggregatedGameData['Missed_Shots_away'] = aggregatedGameData['Shot_Attmpt_away'] - aggregatedGameData['Shot_Make_away']

    aggregatedGameData.fillna(0, inplace=True)

def discretizeFeatures(aggregatedGameData, bin_name, event_type):
    """
    Discretize features into categorical bins.

    Replaces raw event count or engineered features with categorical versions based
    on pre-specified bin thresholds from pbp_event_mapping_config.

    Args:
    - aggregatedGameData (pd.DataFrame): A DataFrame containing event counts or engineered features.
    - bin_name (str): The name of the bin set to use (e.g., bin_120_quantile_1) as defined
      in pbp_event_mapping_config
    - event_type (str): One of {'teams', 'ovr', 'eng', 'rbd'}, determining which configuration
      to use for binning logic.

    Returns:
    - pd.DataFrame: A modified version of the input DataFrame with the selected event columns
      replaced by categorical versions.
    """
    aggregatedGameDataCopy = aggregatedGameData.copy()
    new_columns = {}

    # Define which events to process based on the event type
    team_event_names = event_columns_to_sum_teams
    ovr_events = overall_events

    if event_type == 'teams':
        events_to_process = [event for event in events_config if event["name"] in team_event_names]
        cols_to_drop = [f"{event['name']}_home" for event in events_to_process] + \
                       [f"{event['name']}_away" for event in events_to_process]
    elif event_type == 'ovr':
        events_to_process = [event for event in events_config if event["name"] in ovr_events]
        cols_to_drop = [f"{event['name']}" for event in events_to_process]
    elif event_type == 'eng':
        events_to_process = [event for event in engineered_features]
        cols_to_drop = [f"{event['name']}_home" for event in events_to_process] + \
                       [f"{event['name']}_away" for event in events_to_process]
    elif event_type == 'rbd':
        events_to_process = [event for event in additional_categorical_bins]
        cols_to_drop = [f"{event['name']}" for event in events_to_process]

    # Loop through each event and discretize the values using pre-specified bin thresholds from pbp_event_mapping_config
    for event in events_to_process:
        base_event_name = event["name"]
        bins = event.get("bins", {}).get(bin_name, [])
        if not bins:
            continue
        if bins == [1]:  # Special case for binary events
            categories = [0, 1]
            bins_to_use = [0]
        else:
            categories = list(range(len(bins) + 1))
            bins_to_use = bins
        # Apply binning logic depending on whether the event is team-tagged or overall
        if event_type in ['teams', 'eng', 'rbd']:
            suffixes = ['_home', '_away'] if event_type != 'rbd' else ['']
            for suffix in suffixes:
                event_name = base_event_name + suffix
                if event_name in aggregatedGameDataCopy.columns:
                    if event_type in ['teams']:
                        # Use right-closed bins for team events
                        new_columns[event_name] = pd.cut(
                        aggregatedGameDataCopy[event_name],
                        bins=[-1] + bins_to_use + [float("inf")],
                        labels=categories,
                        right=True
                        )
                    else:
                        # Use left-closed bins for engineered events
                        new_columns[event_name] = pd.cut(
                            aggregatedGameDataCopy[event_name],
                            bins=[-1, 0.001] + bins[1:] + [float("inf")],
                            labels=categories,
                            right=False
                        )
        else:
            # Apply binning to overall event columns
            if base_event_name in aggregatedGameDataCopy.columns:
                new_columns[base_event_name] = pd.cut(
                        aggregatedGameDataCopy[base_event_name],
                        bins=[-1] + bins_to_use + [float("inf")],
                        labels=categories,
                        right=True
                    )

    discretizedData = pd.DataFrame(new_columns, index=aggregatedGameDataCopy.index)
    discretizedDataFinal = pd.concat([aggregatedGameDataCopy.drop(columns=cols_to_drop), discretizedData], axis=1)

    return discretizedDataFinal

def markMomentumEventsInIntervals(discretizedSeasonData, momentumEvents, intensity, time_window):
    """
    Mark the time intervals where momentum events were realized momentum events across the discretized season data.

    This function tags the interval when each momentum event was realized (first crossed the explosivness threshold
    after the minimum buildup duration) based on precomputed momentum episodes. It assigns a categorical level
    (1 = short, 2 = long) depending on the duration of the momentum event

    Args:
    - discretizedSeasonData (pd.DataFrame): Discreteized season-level data
    - momentumEvents (pd.DataFrame): DataFrame containing momentum events
    - intensity (float): The intensity threshold used to identify this class of momentum
      event (e.g., 0.05 or 0.07). Used to name the output column.
    - time_window (int): Length of each time window in seconds (e.g., 120).

    Returns:
    - pd.DataFrame: Updated season DataFrame with columns that track the intervals where a momentum event was realized
        for home or away teams.
    """
    # Initialize columns to store the flags
    discretizedSeasonData[f'home_momentum{intensity}'] = 0
    discretizedSeasonData[f'away_momentum{intensity}'] = 0

    # Create a level to indicate if the duration of the momentum event was in the 90th percentile of momentum event
    # duration
    if intensity == 0.05:
        duration_bucket_thresh = 60
    if intensity == 0.07:
        duration_bucket_thresh = 30


    for _, event in momentumEvents.iterrows():
        # Calculate the time windows for the momentum event start and end
        start_window = ((event['momentum_start_time'] - 1) // time_window) + 1
        end_window = ((event['end_time'] - 1) // time_window) + 1
        game_id = event['game_id']

        if event['duration'] < duration_bucket_thresh:
            duration_bucket = 1
        if event['duration'] >= duration_bucket_thresh:
            duration_bucket = 2

        # Mark the appropriate columns in the season data
        if event['home_or_away'] == 'Home Momentum':
            discretizedSeasonData.loc[(discretizedSeasonData['gameid'] == game_id) & (
                    discretizedSeasonData['time_window'] == start_window), f'home_momentum{intensity}'] = duration_bucket
        if event['home_or_away'] == 'Away Momentum':
            discretizedSeasonData.loc[(discretizedSeasonData['gameid'] == game_id) & (
                    discretizedSeasonData['time_window'] == start_window), f'away_momentum{intensity}'] = duration_bucket

    return discretizedSeasonData

def addLaggedColumns(discretizedGamaData, num_lags):
    """
    Add lagged versions of all event and feature columns across time windows in a game.

    Args:
    - discretizedGamaData (pd.DataFrame): DataFrame containing discretized event and feature
      columns for a single game
    - num_lags (int): Number of previous time windows to lag

    Returns:
    - pd.DataFrame: Original DataFrame with additional columns of the form 'feature_lag1',
      'feature_lag2', etc
    """
    # Ensure the DataFrame is sorted by 'time_window' to correctly apply lags
    discretizedGamaData = discretizedGamaData.sort_values(by=['gameid',  'time_window'])

    # Identify columns to lag (excluding identifiers and 'time_window')
    columns_to_lag = [
        col for col in discretizedGamaData.columns
        if 'SEASON_TO_DATE' not in col and col not in ['gameid', 'time_window']
    ]

    lagged_columns = {}

    # Apply lags for the specified number of time windows
    for col in columns_to_lag:
        for lag in range(1, num_lags + 1):
            lagged_col_name = f"{col}_lag{lag}"
            lagged_columns[lagged_col_name] = discretizedGamaData.groupby(['gameid'])[col].shift(lag)

    laggedDf = pd.DataFrame(lagged_columns, index=discretizedGamaData.index)
    discretizedGamaDataLagged = pd.concat([discretizedGamaData, laggedDf], axis=1)

    return discretizedGamaDataLagged

def generateNonDiscretizedFeaturesSeason(season, time_window):
    """
        Generate non-discretized event and feature data for an entire season.

        This function loads pre-processed play-by-play data for a given season and processes
        each game to compute raw (non-discretized) team-level and overall game features
        within fixed-length time intervals. It applies event mapping, aggregation,
        feature engineering, and formatting.

        Args:
        - season (str): The season identifier (e.g., '2021', '2022') used to locate the input file.
        - time_window (int): Length of each analysis interval in seconds (e.g., 120 for 2-minute windows).

        Returns:
        - pd.DataFrame: Combined DataFrame containing raw features and event counts per team
          per time window, with one row per interval per game. This output is used as input
          for downstream discretization.
    """
    base_dir = os.path.dirname(__file__)
    open_path = os.path.join(base_dir, '..', 'data', 'pbp_data', f'processed_pbp_{season}.pkl')
    open_path = os.path.abspath(open_path)
    processedSeasonData = pd.read_pickle(open_path)

    game_ids = processedSeasonData['gameid'].unique().tolist()
    total_games = len(game_ids)

    season_data = []
    progress_counter = 0
    with progressbar.ProgressBar(max_value=total_games, widgets=[progressbar.Percentage(), " ",
                                                                 progressbar.GranularBar(), " ",
                                                                 progressbar.AdaptiveETA(), ]) as bar:
        for game in game_ids:
            gameData = processedSeasonData[(processedSeasonData['gameid'] == game)].copy()
            gameData = gameData.sort_values(by=['eventClock', 'orderNumber']).reset_index(drop=True)
            gameDataEvents = gameData.apply(mapEvents, axis=1)
            teamDataEvents = sumTeamEventsWithinIntervals(gameDataEvents, time_window)
            teamDataEvents = calculateEngineeredEvents(teamDataEvents)
            teamDataEvents = formatEventsWithTeams(teamDataEvents)
            calculateTeamTaggedFeatures(teamDataEvents)
            overallDataEvents = sumOverallEventsWithinIntervals(gameDataEvents, time_window)
            game_sums = teamDataEvents.merge(overallDataEvents, on='time_window', how='left')
            season_data.append(game_sums)
            progress_counter = progress_counter + 1
            bar.update(progress_counter)

    seasonDf = pd.concat(season_data)

    return seasonDf

def generateDiscretizedModelInputsSeason(season, intensity_thresholds, momentum_time_threshold, event_window, bins, num_lags):
    """
        Generate fully discretized model inputs with lagged features for a full season.

        Loads event-level data, engineered features, lineup statistics, momentum
        events, and interval momentum for an entire season. All features are discretized
        and merged into a unified dataset with lagged versions for each time interval

        Args:
        - season (str): Season identifier (e.g., '2023') used to locate input files.
        - intensity_thresholds (list of float): List of momentum explosiveness thresholds (e.g., [0.05, 0.07])
          for identifying and labeling momentum events.
        - momentum_time_threshold (int): Minimum duration (in seconds) used to define valid  momentum events.
        - event_window (int): Length of each game interval in seconds (e.g., 120).
        - bins (str): The name of the bin set to use (e.g., bin_120_quantile_1) as defined
            in pbp_event_mapping_config
        - num_lags (int): Number of past intervals to include for each lagged feature.

        Returns:
        - pd.DataFrame: Final discretized model inputs with lagged features for a full season.
            - Discretized event and engineered features (team-level and overall)
            - Discretized momentum explosiveness category
            - Season-to-date lineup statistics and lineup cluster
            - Realized momentum event markers
            - Lagged versions of all features for temporal modeling
    """
    base_dir = os.path.dirname(__file__)
    event_sums_season_path = os.path.abspath(os.path.join(
        base_dir, '..', 'data', 'pbp_data', f'window_event_counts_{event_window}s_{season}.pkl'))
    seasonEventSums = pd.read_pickle(event_sums_season_path)

    # Load the summed events and engineered events for each time interval for all games in the season and discretize
    seasonDataIntervalEventsDiscrete = discretizeFeatures(seasonEventSums, bins, 'teams')
    seasonDataIntervalEventsDiscrete = discretizeFeatures(seasonDataIntervalEventsDiscrete, bins, 'eng')
    seasonDataIntervalEventsDiscrete = discretizeFeatures(seasonDataIntervalEventsDiscrete, bins, 'ovr')
    seasonDataIntervalEventsDiscrete = discretizeFeatures(seasonDataIntervalEventsDiscrete, bins, 'rbd')

    # Load the season-to-date lineup features and lineup clusters
    lineup_all_seasons_path = os.path.abspath(os.path.join(
        base_dir, '..', 'data', 'lineup_stats', f'{event_window}s',
        f'lineup_stats_with_clusters_{event_window}s_ALL_SEASONS.pkl'))
    lineupData = pd.read_pickle(lineup_all_seasons_path)
    lineupData = lineupData.rename(columns={"GAME_ID": "gameid"})
    lineup_columns = ['gameid', 'time_window', 'lineup_cluster_away', 'lineup_cluster_home'] + [col for col in lineupData.columns if 'BUCKET' in col]
    lineupData = lineupData[lineup_columns]

    # Load the discretized interval momentum explosiveness feature for each time interval for all games in the season
    intervalExplosivenessSeasonPath = os.path.abspath(os.path.join(
        base_dir, '..', 'data', 'momentum', f'{event_window}s', f'window_intensities_{event_window}s_{season}.pkl'))
    intervalExplosivenessData = pd.read_pickle(intervalExplosivenessSeasonPath)
    intervalExplosiveness_columns = ['gameid', 'time_window', 'momentum_category']
    intervalExplosivenessData = intervalExplosivenessData[intervalExplosiveness_columns]

    #Load the momentum events that were identified over an entire season and mark them in each game interval
    momentum_data = {}
    for intensity in intensity_thresholds:
        momentum_episodes_season_path = os.path.abspath(os.path.join(
            base_dir, '..', 'data', 'momentum', f'{momentum_time_threshold}s',
            f'momentum_{momentum_time_threshold}s_{intensity}int_{season}.pkl'))
        momentum_data[intensity] = pd.read_pickle(momentum_episodes_season_path)

    for i, (intensity, momentumEventsData) in enumerate(momentum_data.items()):
        if i == 0:
            allData = markMomentumEventsInIntervals(seasonDataIntervalEventsDiscrete, momentumEventsData, intensity,
                                                            event_window)
        else:
            allData = markMomentumEventsInIntervals(allData, momentumEventsData, intensity, event_window)

    # Merge all the loaded data in discretized form together
    allData = allData.merge(intervalExplosivenessData, on = ['gameid','time_window'],how='left')
    allData = allData.merge(lineupData, on=['gameid', 'time_window'], how='left')
    allData = allData.drop('smaller_time_window_away', axis=1)
    allData = allData.rename(columns={'smaller_time_window_home': 'smaller_time_window'})

    allDataWithLags = addLaggedColumns(allData, num_lags)

    return allDataWithLags

def generateDiscretizedModelInputsMultipleSeason(seasons, intensity_thresholds, momentum_time_threshold, event_window, bins, num_lags):
    """
        Generate discretized and lagged model inputs for multiple seasons.

        This wrapper function applies `generateDiscretizedModelInputsSeason` to each season
        in the input list and concatenates the resulting DataFrames into a single dataset.

        Args:
        - seasons (list of str): List of season identifiers (e.g., ['2021', '2022']).
        - intensity_thresholds (list of float): Momentum explosiveness thresholds to apply.
        - momentum_time_threshold (int): Minimum duration (in seconds) for defining momentum events.
        - event_window (int): Length of each analysis interval in seconds.
        - bins (str): The name of the bin set to use (e.g., bin_120_quantile_1) as defined
            in pbp_event_mapping_config
        - num_lags (int): Number of lagged intervals to include.

        Returns:
        - pd.DataFrame: Combined dataset with discretized and lagged features across all specified seasons.
    """
    seasonDfs = []
    for season in seasons:
        season_df = generateDiscretizedModelInputsSeason(season, intensity_thresholds, momentum_time_threshold, event_window, bins, num_lags)
        seasonDfs.append(season_df)
    allSeasonsDf = pd.concat(seasonDfs, ignore_index=True)

    return allSeasonsDf