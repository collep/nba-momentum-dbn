import pandas as pd
import numpy as np

def calculateExplosiveness(pbpGameData, startTime, time_min, time_max):
    """
    Calculates momentum explosiveness (intensity) for a given play-by-play game window.

    Momentum explosiveness is defined as the change in score differential over time,
    normalized by duration. This function evaluates the momentum explosiveness
    between the start time and all the time points in a specified forward-looking window.

    Args:
        pbpGameData (pd.DataFrame): Play-by-play data for a single game.
        startTime (int): The start time in seconds from which explosiveness is measured.
        time_min (int): The minimum duration (in seconds) to look ahead from startTime.
                        This corresponds to the minimum buildup duration required to
                        qualify as a potential momentum event.
        time_max (int): The maximum duration (in seconds) to evaluate explosiveness beyond startTime.

    Returns:
        pd.Series: A series indexed by time (in seconds) containing momentum intensities (float).
    """
    start_time = int(startTime)
    time_min = int(time_min)
    time_max = int(time_max)

    # Define the time range to evaluate, bounded by start_time + time_min and start_time + time_max
    time_range = range(start_time + time_min, min(start_time + time_max, int(pbpGameData['eventClock'].max())) + 1)

    # Calculate the score difference at the time of each event in the play-by-play log
    pbpGameData['score_diff'] = pbpGameData['scoreHome'].astype(int) - pbpGameData['scoreAway'].astype(int)

    # Find the latest score difference for each event time
    latestScores = pbpGameData.groupby('eventClock')['score_diff'].last()

    # Determine the score difference immediately before the start_time
    scoreDiffStart = latestScores[latestScores.index < start_time].iloc[-1] if not latestScores[
        latestScores.index < start_time].empty else 0

    # Fill in the score diffs for all the time points that are in between play-by-play events
    scoreDiffs = latestScores.reindex(time_range, method='ffill')

    if scoreDiffs.empty:
        return pd.Series(dtype='float64')

    # Calculate momentum intensity/explosiveness as the change in score difference normalized by time
    intensities = (scoreDiffs - scoreDiffStart) / (scoreDiffs.index - start_time)

    return intensities

def findGameMomentumEvents(pbpGameData, explosivenessThreshold, time_min, time_max):
    """
    Identifies and aggregates momentum events within a single NBA game based on momentum explosiveness.

    Momentum events are detected by evaluating scoring differential intensity (explosiveness) over time.
    A momentum episode is triggered when the normalized change in score exceeds the specified threshold
    and is sustained for at least `time_min` seconds (minimum buildup duration). Events are labeled as either
    home or away momentum based on direction of scoring.

    Args:
        pbpGameData (pd.DataFrame): Play-by-play data for a single NBA game.
        explosivenessThreshold (float): Minimum normalized change in score required to flag a potential momentum event.
        time_min (int): The minimum duration (in seconds) to look ahead from startTime.
                        This corresponds to the minimum buildup duration required to
                        qualify as a potential momentum event.
        time_max (int): Maximum lookahead window (in seconds) for calculating explosiveness at each time step.

    Returns:
        pd.DataFrame: A DataFrame of aggregated momentum events containing:
                      'game_id', 'build_up_start_time', 'momentum_start_time', 'end_time', 'duration',
                      'avg_intensity', 'peak_intensity_max', 'peak_intensity_min', and 'home_or_away'.
                      Returns an empty DataFrame if no momentum episodes are detected.
    """
    momentumEvents = []

    max_event_time = pbpGameData['eventClock'].max()
    game_duration = int(max(max_event_time, 2880))
    game_id = pbpGameData['gameid'].max()

    current_time = 1  # Start analyzing from the first second of the game

    # Loop through each second of the game to identify potential momentum event sequences
    while current_time <= game_duration:
        # Calculate momentum explosiveness values starting from the current time
        intensities = calculateExplosiveness(pbpGameData, current_time, time_min, time_max)

        # Identify time points where the explosiveness exceeds the threshold
        significantMomentumIntervals = intensities[abs(intensities) >= explosivenessThreshold]

        if not significantMomentumIntervals.empty:
            # Initialize momentum metadata for first detected interval
            firstSignificant_time = significantMomentumIntervals.index[0]
            first_intensity = significantMomentumIntervals.iloc[0]
            momentum_direction = 'negative' if first_intensity < 0 else 'positive'
            momentumStartTime = None

            # Iterate through all significant intervals
            for moment_time, intensity in significantMomentumIntervals.items():
                current_direction = 'negative' if intensity < 0 else 'positive'

                # Set the official momentum start time (if sustained for at least time_min seconds)
                if momentumStartTime is None and (
                        moment_time - current_time) >= time_min and current_direction == momentum_direction:
                    momentumStartTime = moment_time

                # Detect and break on momentum reversal (e.g., shift from home to away momentum)
                if (moment_time - firstSignificant_time) > 1 and current_direction != momentum_direction:
                    home_or_away = 'Away Momentum' if intensity < 0 else 'Home Momentum'
                    momentumEvents.append({
                        'game_id': game_id,
                        'build_up_start_time': current_time,
                        'momentum_start_time': momentumStartTime,
                        'end_time': moment_time,
                        'intensity': intensity,
                        'home_or_away': home_or_away
                    })
                    current_time = moment_time
                    break

                # Record the ongoing momentum event interval
                home_or_away = 'Away Momentum' if intensity < 0 else 'Home Momentum'
                momentumEvents.append({
                    'game_id': game_id,
                    'build_up_start_time': current_time,
                    'momentum_start_time': momentumStartTime,
                    'end_time': moment_time,
                    'intensity': intensity,
                    'home_or_away': home_or_away
                })

                momentum_direction = current_direction
                firstSignificant_time = moment_time

            # Advance to just beyond the last evaluated time point
            current_time = significantMomentumIntervals.index[-1] + 1
        else:
            # No momentum event detected at this second, move forward one second
            current_time += 1

    momentumDataframe = pd.DataFrame(momentumEvents)

    if not momentumDataframe.empty:
        # Aggregate momentum episodes based on their build-up start time
        aggregatedDataframe = momentumDataframe.groupby('build_up_start_time').agg(
            game_id=pd.NamedAgg(column="game_id", aggfunc='first'),
            momentum_start_time=pd.NamedAgg(column="momentum_start_time", aggfunc='min'),
            end_time=pd.NamedAgg(column="end_time", aggfunc='max'),
            peak_intensity_max=pd.NamedAgg(column="intensity", aggfunc='max'),
            peak_intensity_min=pd.NamedAgg(column="intensity", aggfunc='min'),
            avg_intensity=pd.NamedAgg(column="intensity", aggfunc='mean'),
            home_or_away=pd.NamedAgg(column="home_or_away", aggfunc='max')).reset_index()

        # Recalculate the duration based on the momentum start and end times
        aggregatedDataframe['duration'] = aggregatedDataframe['end_time'] - aggregatedDataframe['momentum_start_time']

        column_order = ['game_id', 'build_up_start_time', 'momentum_start_time', 'end_time', 'duration',
                        'avg_intensity', 'peak_intensity_max', 'peak_intensity_min', 'home_or_away']
        aggregatedDataframe = aggregatedDataframe[column_order]

        return aggregatedDataframe
    else:
        # Return empty DataFrame if no events detected
        return momentumDataframe

def calculateIntervalExplosivenessCategorical(pbpGameData, time_window):
    """
    Calculates and categorizes momentum explosiveness over fixed time intervals for a single NBA game.

    This function segments the play-by-play timeline into fixed-length intervals (e.g., 120 seconds),
    computes the change in score differential over each interval, and normalizes it by the interval duration to
    produce a momentum explosiveness value. These values are then discretized into categorical bins.

    This categorical output corresponds to the model feature `Interval Momentum Explosiveness`,
    which tracks continuous momentum patterns across the full game — including intervals where no
    formal momentum event is detected.

    Args:
        pbpGameData (pd.DataFrame): Play-by-play data for a single game
        time_window (int): Length of each time interval in seconds (e.g., 120).

    Returns:
        pd.DataFrame: A DataFrame containing one row per time interval with the following columns:
                      - 'gameid'
                      - 'time_window' (interval number)
                      - 'momentum_intensity' (momentum explosiveness calcualted as the score diff rate)
                      - 'momentum_category' (binned version of intensity; values 1–10)
    """
    # Create time window columns
    time_window_column = ((pbpGameData['eventClock'] - 1) // time_window) + 1
    window_start_column = (time_window_column - 1) * time_window
    window_end_column = time_window_column * time_window

    # Adjust time_window for the maximum event time
    max_event_time = pbpGameData['eventClock'].max()
    if max_event_time % time_window == 0:
        time_window_column.loc[pbpGameData['eventClock'] == max_event_time] = max_event_time // time_window

    # Adjust `window_end` if it exceeds `max_time`
    window_end_column = np.where(window_end_column > max_event_time, max_event_time, window_end_column)

    # Concatenate new columns to game play-by-play data
    gameData = pd.concat([pbpGameData, time_window_column.rename('time_window'),
                               window_start_column.rename('window_start'),
                               pd.Series(window_end_column, name='window_end')], axis=1)

    gameData = gameData.assign(
        time_window=np.where(gameData['time_window'] == 0, 1, gameData['time_window']),
        window_start=np.where(gameData['time_window'] == 0, 0, gameData['window_start']),
        window_end=np.where(gameData['time_window'] == 0, time_window, gameData['window_end'])
    )

    # Calculate score differences
    gameData['score_diff'] = gameData['scoreHome'].astype(int) - gameData['scoreAway'].astype(int)

    # Calculate momentum explosiveness for each time interval/window
    momentumIntensities = gameData.groupby(['gameid', 'time_window']).apply(
        lambda x: (x['score_diff'].iloc[-1] - x['score_diff'].iloc[0]) / (
                    x['window_end'].iloc[0] - x['window_start'].iloc[0])
    ).reset_index(name='momentum_intensity')

    # Define the bins and labels for momentum intensity categorization
    bins = [-np.inf, -0.08, -0.06, -0.04, -0.02, 0, 0.02, 0.04, 0.06, 0.08, np.inf]
    labels = [1,2,3,4,5,6,7,8,9,10]

    momentumIntensities['momentum_category'] = pd.cut(momentumIntensities['momentum_intensity'], bins=bins,
                                                       labels=labels)

    return momentumIntensities

def momentumTeamId(momentumTeam,home_team_id,away_team_id):
    """
        Maps a momentum event to the corresponding team ID.

        Args:
            momentumTeam (str): Momentum direction label ('Home Momentum' or 'Away Momentum').
            home_team_id (int): Team ID for the home team.
            away_team_id (int): Team ID for the away team.

        Returns:
            int: The team ID corresponding to the momentum team.
        """
    if momentumTeam == 'Away Momentum':
        return away_team_id
    elif momentumTeam == 'Home Momentum':
        return home_team_id

def findSeasonMomentumEvents(processedPbpSeasonData, explosivenessThreshold, time_min, time_max):
    """
        Identifies momentum events across all games in a season.

        For each game in the season, this function:
        - Detects momentum episodes using the specified explosiveness threshold and duration parameters.

        This function expects input from `processPbpSeasonData`, where play-by-play data has already been
        preprocessed with event clocks, possession markers, and lineups.

        Args:
            processedPbpSeasonData (pd.DataFrame): Processed play-by-play data for all games in a season.
            explosivenessThreshold (float): Minimum momentum explosiveness required to define a momentum event.
            time_min (int): The minimum duration (in seconds) to look ahead from startTime.
                        This corresponds to the minimum buildup duration required to
                        qualify as a potential momentum event.
            time_max (int): Maximum window (in seconds) to evaluate momentum explosiveness from any given point.

        Returns:
            pd.DataFrame: A DataFrame of all momentum episodes in the season.
        """
    game_ids = processedPbpSeasonData['gameid'].unique().tolist()
    gameMomentumEvents = []

    for game in game_ids:
        gameData = processedPbpSeasonData[(processedPbpSeasonData['gameid'] == game)].copy()
        home_team_id = gameData['HOME_TEAM_ID'].unique()[0]
        away_team_id = gameData['AWAY_TEAM_ID'].unique()[0]
        gameData = gameData.sort_values(by=['eventClock', 'orderNumber']).reset_index(drop=True)
        momentumEvents = findGameMomentumEvents(gameData, explosivenessThreshold, time_min, time_max)
        if not momentumEvents.empty:
            momentumEvents['TEAM_ID'] = momentumEvents.apply(
                lambda x: momentumTeamId(x['home_or_away'], home_team_id, away_team_id), axis=1)
            gameMomentumEvents.append(momentumEvents)

    seasonMomentumEvents = pd.concat(gameMomentumEvents, ignore_index=True)

    return seasonMomentumEvents

def seasonIntervalExplosiveness(processedPbpSeasonData, time_window):
    """
        Calculates and categorizes interval-based momentum explosiveness for all games in a season.

        This categorical output corresponds to the model feature `Interval Momentum Explosiveness`,
        which tracks continuous momentum patterns across the full game — including intervals where no
        formal momentum event is detected.

        Args:
            processedPbpSeasonData (pd.DataFrame): Preprocessed season-long play-by-play data,
                                                   with eventClock and score columns available.
            time_window (int): Length of each interval in seconds.

        Returns:
            pd.DataFrame: A DataFrame with one row per game-time interval that contains the momentum explosiveness
            in categorical form for all games in a season.
        """
    game_ids = processedPbpSeasonData['gameid'].unique().tolist()
    gameMomentumEvents = []

    for game in game_ids:
        gameData = processedPbpSeasonData[(processedPbpSeasonData['gameid'] == game)].copy()
        gameData = gameData.sort_values(by=['eventClock', 'orderNumber']).reset_index(drop=True)
        intervalExplosiveness = calculateIntervalExplosivenessCategorical(gameData, time_window)
        gameMomentumEvents.append(intervalExplosiveness)

    seasonIntervalExplosiveness = pd.concat(gameMomentumEvents, ignore_index=True)

    return seasonIntervalExplosiveness