import requests
import json
import difflib
import pandas as pd
import numpy as np
from nba_api.stats.endpoints import leaguegamefinder, boxscoretraditionalv2, cumestatsteamgames
from nba_api.stats.static import teams
import progressbar
import time
import warnings


def retry(func, retries=3):
    """
        Wraps a function with retry logic for handling request failures.

        Retries the given function up to `retries` times if it raises a
        `requests.exceptions.RequestException`. Waits 30 seconds between attempts.

        Args:
            func (Callable): The function to be wrapped and retried.
            retries (int, optional): Maximum number of retry attempts.

        Returns:
            Callable: A wrapped function that includes retry logic.
        """
    def retryWrapper(*args, **kwargs):
        attempts = 0
        while attempts < retries:
            try:
                return func(*args, **kwargs)
            except requests.exceptions.RequestException as e:
                print(e)
                time.sleep(30)
                attempts += 1

    return retryWrapper

@retry
def getGamePbp(game_id):
    """
    Retrieves play-by-play logs for a specific NBA game from the NBA.com liveData endpoint.

    Args:
        game_id (str): NBA game ID string (e.g., '0022200001').

    Returns:
        pd.DataFrame: A DataFrame containing all play-by-play events for the specified game
    """
    headers = {
        'Connection': 'keep-alive',
        'Accept': 'application/json, text/plain, */*',
        'x-nba-stats-token': 'true',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36',
        'x-nba-stats-origin': 'stats',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-Mode': 'cors',
        'Referer': 'https://stats.nba.com/',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9',
    }

    playByPlayUrl = "https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_" + game_id + ".json"

    response = requests.get(url=playByPlayUrl, headers=headers).json()
    playByPlayData = response['game']['actions']
    playByPlayDf = pd.DataFrame(playByPlayData)
    playByPlayDf['gameid'] = game_id

    return playByPlayDf


@retry
def getSeasonGameIds(season):
    """
    Retrieves all NBA game IDs for a given season using the nba_api package.

    Args:
        season (str): NBA season string in format 'YYYY-YY' (e.g., '2020-21').

    Returns:
        list: A list of unique game ID strings for all regular season games.
    """
    gamefinder = leaguegamefinder.LeagueGameFinder(season_nullable=season,
                                                   league_id_nullable='00',
                                                   season_type_nullable='Regular Season')
    games = gamefinder.get_data_frames()[0]
    game_ids = games['GAME_ID'].unique().tolist()

    return game_ids

def getSeasonScheduleFrame(season):
    """
    Builds a complete schedule DataFrame for a given NBA season, including team matchups and game results.

    For each game in the season, the function extracts:
    - Home and away team names and IDs
    - Game dates and matchups
    - The winning team for each game (home or away)

    Args:
        season (str): NBA season string in format 'YYYY-YY' (e.g., '2020-21').

    Returns:
        pd.DataFrame: A DataFrame containing one row per game with columns for game ID, team names and IDs,
                      matchup date, and home/away win indicators.
    """
    def getGameDate(matchup):
        return matchup.partition(' at')[0][:10]

    def getHomeTeam(matchup):
        return matchup.partition(' at')[2]

    def getAwayTeam(matchup):
        return matchup.partition(' at')[0][10:]

    def getTeamIDFromNickname(nickname):
        return teamLookup.loc[
            teamLookup['nickname'] == difflib.get_close_matches(nickname, teamLookup['nickname'], 1)[0]].values[0][0]

    @retry
    def getRegularSeasonSchedule(season, teamID):
        teamGames = cumestatsteamgames.CumeStatsTeamGames(league_id='00', season=season,
                                                          season_type_all_star='Regular Season',
                                                          team_id=teamID).get_normalized_json()

        teamGames = pd.DataFrame(json.loads(teamGames)['CumeStatsTeamGames'])
        teamGames['SEASON'] = season
        return teamGames

    teamLookup = pd.DataFrame(teams.get_teams())
    scheduleFrame = pd.DataFrame()

    #Build the league season schedule by pulling the individual team schedules
    for id in teamLookup['id']:
        time.sleep(1)
        scheduleFrame = pd.concat([scheduleFrame,getRegularSeasonSchedule(season, id)],ignore_index=True)

    #Enrich scehdule with metadata
    scheduleFrame['GAME_DATE'] = pd.to_datetime(scheduleFrame['MATCHUP'].map(getGameDate))
    scheduleFrame['HOME_TEAM_NICKNAME'] = scheduleFrame['MATCHUP'].map(getHomeTeam)
    scheduleFrame['HOME_TEAM_ID'] = scheduleFrame['HOME_TEAM_NICKNAME'].map(getTeamIDFromNickname)
    scheduleFrame['AWAY_TEAM_NICKNAME'] = scheduleFrame['MATCHUP'].map(getAwayTeam)
    scheduleFrame['AWAY_TEAM_ID'] = scheduleFrame['AWAY_TEAM_NICKNAME'].map(getTeamIDFromNickname)
    scheduleFrame = scheduleFrame.drop_duplicates()  # There's a row for both teams for each game, only need 1


    #Populate the schedule with the team that won the game
    winFinder = leaguegamefinder.LeagueGameFinder(season_nullable=season)
    winFinder = winFinder.get_data_frames()[0]
    winFinder = winFinder[winFinder['WL']=='W']
    winFinder = winFinder[['GAME_ID','TEAM_ID','WL']]

    scheduleFrame= scheduleFrame.merge(winFinder, how='left', left_on=['GAME_ID','HOME_TEAM_ID'],
                        right_on = ['GAME_ID', 'TEAM_ID'], indicator=True)

    scheduleFrame['HOME_WIN'] = np.where(scheduleFrame['_merge'] == 'both', 1, 0)
    scheduleFrame['AWAY_WIN'] = np.where(scheduleFrame['_merge'] == 'both', 0, 1)
    scheduleFrame['WIN_TEAM_ID'] = np.where(scheduleFrame['_merge'] == 'both', scheduleFrame['HOME_TEAM_ID'],
                                        scheduleFrame['AWAY_TEAM_ID'])

    scheduleFrame.drop(columns=['_merge','WL'], inplace=True)
    scheduleFrame = scheduleFrame.reset_index(drop=True)
    scheduleFrame= scheduleFrame.rename(columns={"GAME_ID":"gameid"})

    return scheduleFrame

def eventClock(pbpRow):
    """
    Computes the elapsed game time in seconds when a play-by-play event occured.

    Uses the period number and game clock to calculate how many seconds have passed
    since the start of the game for the given event. Supports regulation and overtime periods.

    Args:
        pbp_row (pd.Series): A row from a play-by-play DataFrame containing 'period' and 'clock' fields.

    Returns:
        float: Time in seconds since the start of the game when the event occurred.
    """
    period = pbpRow["period"]
    clock = pbpRow["clock"]

    #Parse the game clock in the play-by-play log that has the for 'PT12M00.00S'. The clock is for the specific
    # period/quarter the event occured in
    periodMinute = float(clock[2:4])
    periodSeconds = float(clock[5:10])

    if period < 5:
        if periodMinute == float(12):
            eventClock = float((period-1)*60.0*12.0)
        else:
            eventClock = float((period-1)*60.0*12.0) + float((12.0-periodMinute-1)*60.0) + float((60.0-periodSeconds))

    #Special case for overtime game
    if period > 4:
        if periodMinute == float(5):
            eventClock = float(4.0*12.0*60.0 + (period-4-1)*60.0*5.0)
        else:
            eventClock = float(4.0*12.0*60.0) + float((period-4-1)*60.0*5.0) + float((5-periodMinute-1)*60.0) + float((60.0-periodSeconds))

    return eventClock


def newPossessionIndicator(pbpGameData):
    """
        Identifies and tags the start of new offensive possessions in NBA play-by-play logs.

        The play-by-play logs mark the start of new offensive possessions ('possession') but need to be slightly ammended.
        New possesions that start with under 2 seconds left in a quarter are not counted unless they end with points
            being scored (https://darrylblackport.com/posts/2019-04-03-why-pbpstats-possession-counts-lower/)
        A new column 'new_offense_poss' is added to the DataFrame to indicate which events begin possessions.

        Args:
            pbp_game_data (pd.DataFrame): A play-by-play DataFrame for a single game.

        Returns:
            pd.DataFrame: The input DataFrame with an added 'new_offense_poss' column that flags new possessions.
        """
    def periodClockToSeconds(clock_str):
        if isinstance(clock_str, str):
            minutes, seconds = clock_str.replace('PT', '').replace('S', '').split('M')
            total_seconds = int(minutes) * 60 + float(seconds)
            return total_seconds
        return None

    def checkFinalSeconds(pbp_game_data, row, possession):
        for i in range(row.name + 1, len(pbp_game_data)):
            next_row = pbp_game_data.iloc[i]

            # If the quarter ends, break
            if next_row['actionType'] == 'period' and next_row['subType'] == 'end':
                return None

            # If the other team gains possession, break
            if next_row['possession'] != possession:
                return None

            # If the current team scores, keep as a new possession
            if next_row['shotResult'] == 'Made':
                return possession
        return None

    def possessionChange(row, next_row_possession_holder):
        if (row['actionType'] == 'period') and (row['subType'] == 'start'):
            if row['period'] == 1:
                return row['next_possession']
            else:
                return row['possession']

        # Check if a delayed possession change was flagged by the previous row
        # This typically happens after a missed shot where the actual possession switch
        # is deferred until a defensive rebound is secured on the next event
        if deferred_possesion_flag[0] is not None:
            poss_return = deferred_possesion_flag[0] # Retrieve the delayed possession team
            deferred_possesion_flag[0] = None # Clear the flag after using it
            if row['period_seconds'] <= 2:
                return checkFinalSeconds(pbpGameData, row, poss_return)
            return poss_return

        if row['possession'] != row['next_possession']:
            if row['actionType'] == 'substitution':
                return None

            # If the current row shows a missed shot and the next event is a defensive rebound, do not immediately
            # assign a new possession. Instead, flag it to be handled in the next row, once the rebound is confirmed.
            if row['next_event'] == 'defensive' and row['shotResult'] == 'Missed':
                deferred_possesion_flag[0] = row['next_possession']
                return None

            # If the possession change happens in the final 2 seconds of a period
            if row['period_seconds'] <= 2:
                return checkFinalSeconds(pbpGameData, row, row['next_possession'])

            # Otherwise, mark it as a new possession
            return row['next_possession']

        return None

    pbpGameData['period_seconds'] = pbpGameData['clock'].apply(periodClockToSeconds)
    pbpGameData['next_possession'] = pbpGameData['possession'].shift(-1)
    pbpGameData['next_event'] = pbpGameData['subType'].shift(-1)

    # Temporary flag to defer a possession change decision across rows.
    # Used to delay assigning possession after missed shots until the rebound is confirmed.
    deferred_possesion_flag = [None]

    # Apply function to mark new possessions
    pbpGameData['new_offense_poss'] = pbpGameData.apply(
        lambda row: possessionChange(row, deferred_possesion_flag),axis=1
    )

    pbpGameData.drop(['next_possession', 'next_event'], axis=1, inplace=True)

    return pbpGameData



@retry
def getStartingLineups(game_id, HOME_TEAM_ID,AWAY_TEAM_ID):
    """
        Retrieves starting player IDs for both home and away teams in a given NBA game.

        Args:
            game_id (str): NBA game ID string (e.g., '0022300001').
            HOME_TEAM_ID (int): Team ID for the home team.
            AWAY_TEAM_ID (int): Team ID for the away team.

        Returns:
            tuple[list, list]: Two lists of player IDs — one for home starters, one for away starters.
        """
    boxscore = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
    playerStats = boxscore.get_data_frames()[0]
    time.sleep(0.75)

    starters = playerStats[playerStats['START_POSITION'].isin(['F', 'C', 'G'])]
    homeStarters = starters[starters['TEAM_ID'] == HOME_TEAM_ID]['PLAYER_ID'].tolist()
    awayStarters = starters[starters['TEAM_ID'] == AWAY_TEAM_ID]['PLAYER_ID'].tolist()

    return homeStarters, awayStarters

def trackLineups(pbpGameData):
    """
        Tracks the active on-court players for both teams throughout an NBA game.

        Starting from the initial lineups, it updates the list of players on the court
        based on substitution events in the play-by-play data. It appends two new columns to the
        input DataFrame: 'HomeOnCourt' and 'AwayOnCourt', which contain the list of active player IDs
        for each team at every event.

        Args:
            pbpGameData (pd.DataFrame): Play-by-play data for a single NBA game.

        Returns:
            pd.DataFrame: The same DataFrame with added columns 'HomeOnCourt' and 'AwayOnCourt',
                          reflecting the current lineup on the floor at each row.
        """
    game_id = pbpGameData['gameid'].unique()[0]
    HOME_TEAM_ID = pbpGameData['HOME_TEAM_ID'].unique()[0]
    AWAY_TEAM_ID = pbpGameData['AWAY_TEAM_ID'].unique()[0]

    # Get starting lineups for both teams
    starters = getStartingLineups(game_id, HOME_TEAM_ID,AWAY_TEAM_ID)
    homePlayers = starters[0]
    awayPlayers = starters[1]

    # Initialize empty lineup tracking columns
    pbpGameData['HomeOnCourt'] = [None] * len(pbpGameData)
    pbpGameData['AwayOnCourt'] = [None] * len(pbpGameData)

    # Loop through each event and update lineups based on substitutions
    for index, row in pbpGameData.iterrows():
        actionType = row['actionType']
        subType = row['subType']
        player = row['personId']
        team_id = row['teamId']

        if actionType == 'substitution':
            team_id = int(team_id)
            if team_id == HOME_TEAM_ID:
                if subType == 'out':
                    if player in homePlayers:
                        homePlayers.remove(player)
                    else:
                        warnings.warn(
                            f"Player {player} not found in home_players list during removal at index {index}."
                        )
                if subType == 'in':
                    if player not in homePlayers:
                        homePlayers.append(player)
                    else:
                        warnings.warn(
                            f"Player {player} already in home_players list during addition at index {index}."
                        )
            if team_id == AWAY_TEAM_ID:
                if subType == 'out':
                    if player in awayPlayers:
                        awayPlayers.remove(player)
                    else:
                        warnings.warn(
                            f"Player {player} not found in away_players list during removal at index {index}."
                        )
                if subType == 'in':
                    if player not in awayPlayers:
                        awayPlayers.append(player)
                    else:
                        warnings.warn(
                            f"Player {player} already in away_players list during addition at index {index}."
                        )

        pbpGameData.at[index, 'HomeOnCourt'] = list(homePlayers)
        pbpGameData.at[index, 'AwayOnCourt'] = list(awayPlayers)

    return pbpGameData


def getRawPbpSeason(season):
    """
        Retrieves and compiles raw play-by-play data for all regular season NBA games in a given season.

        Args:
            season (str): NBA season string in format 'YYYY-YY' (e.g., '2022-23').

        Returns:
            pd.DataFrame: A concatenated DataFrame containing play-by-play event data and metadata
                          for all games in the specified season.
        """
    gamefinder = getSeasonScheduleFrame(season)
    game_ids = gamefinder['gameid'].unique().tolist()
    totalGames = len(game_ids)
    seasonData = []

    progressCounter = 0
    with progressbar.ProgressBar(max_value=totalGames, widgets=[ progressbar.Percentage(), " ",
                                                                  progressbar.GranularBar(), " ",
                                                                  progressbar.AdaptiveETA(), ]) as bar:
        for game in game_ids:
            # Pull the play-by-play logs for game
            gameData = getGamePbp(game)

            # Pull additional game metadata from gamefinder
            game_id = gameData['gameid'].unique()[0]
            gameInfo = gamefinder[gamefinder['gameid'] == game_id]
            HOME_TEAM_ID = gameInfo['HOME_TEAM_ID'].unique()[0]
            AWAY_TEAM_ID = gameInfo['AWAY_TEAM_ID'].unique()[0]
            gameDate = gameInfo['GAME_DATE'].unique()[0]

            #Append game metadata
            gameData['SEASON'] = season
            gameData['HOME_TEAM_ID'] = HOME_TEAM_ID
            gameData['AWAY_TEAM_ID'] = AWAY_TEAM_ID
            gameData['GAME_DATE'] = gameDate

            seasonData.append(gameData)
            progress_counter = progress_counter + 1
            bar.update(progress_counter)

    seasonDataframe = pd.concat(seasonData)
    return seasonDataframe

def processPbpSeasonData(RawPbpSeasonData):
    """
        Processes raw play-by-play data for an NBA season.

        This function takes the raw season-level play-by-play data generated by `getRawPbpSeason`
        and performs the following for each game:
        - Computes a continuous event clock (in seconds)
        - Sorts events by game time
        - Identifies new offensive possessions
        - Tracks active lineups on the court

        Args:
            RawPbpSeasonData (pd.DataFrame): A concatenated DataFrame of raw play-by-play data for all games in a season,
                                             as returned by `getRawPbpSeason`.

        Returns:
            pd.DataFrame: A processed play-by-play DataFrame with enriched game context, including event time,
                          possession tags, and on-court lineups.
        """
    if 'value' in RawPbpSeasonData.columns:
        RawPbpSeasonData = RawPbpSeasonData.drop(columns=['value'])

    game_ids = RawPbpSeasonData['gameid'].unique().tolist()
    totalGames = len(game_ids)
    seasonData = []

    progress_counter = 0
    with progressbar.ProgressBar(max_value=totalGames, widgets=[progressbar.Percentage(), " ",
                                                                 progressbar.GranularBar(), " ",
                                                                 progressbar.AdaptiveETA(), ]) as bar:
        for game in game_ids:
            gameData = RawPbpSeasonData[(RawPbpSeasonData['gameid']==game)].copy()
            gameData['eventClock'] = gameData[['period', 'clock']].apply(eventClock, axis=1)
            gameData = gameData.sort_values(by=['eventClock','orderNumber']).reset_index(drop=True)
            gameData = newPossessionIndicator(gameData).sort_values(by=['eventClock','orderNumber']).reset_index(drop=True)
            gameData = trackLineups(gameData)

            seasonData.append(gameData)
            progress_counter = progress_counter + 1
            bar.update(progress_counter)

    processedSeasonDataframe = pd.concat(seasonData)
    return processedSeasonDataframe
