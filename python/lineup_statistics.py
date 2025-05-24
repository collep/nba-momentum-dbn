from python.pbp_data import getSeasonScheduleFrame, retry
import pandas as pd
import numpy as np
from collections import defaultdict
from datetime import datetime, timedelta
import requests
from itertools import combinations
import time
import progressbar
import os

min_minutes_grouping_map = {5: 12, 4: 14.4, 3: 18, 2: 24}

def cleanGroupId(group_id):
    """
    Clean and sort a group ID from the raw lineup data representing a lineup.

    Group IDs are strings formed by concatenating player IDs with dashes (e.g., '201-105-330').
    This function splits the string, converts each ID to an integer, sorts them, and returns the result as a tuple.

    Args:
    - group_id (str): The group ID string to clean and sort.

    Returns:
    - tuple: A sorted tuple of player IDs as integers. Returns an empty tuple if input is empty or malformed.
    """
    group_id = group_id.strip('-')
    cleanedGroupId = tuple(sorted(map(int, filter(None, group_id.split('-'))))) if group_id else ()

    return cleanedGroupId

def generateLineupCombinations(lineup, n):
    """
    Generate all unique combinations of players from a lineup of a specified group size.

    This is used to form sub-lineups (e.g., 2-, 3-, or 4-man groups) from a full 5-player lineup.

    Args:
    - lineup (list): A list of player IDs (or identifiers) representing a full lineup.
    - n (int): The desired number of players in each group combination.

    Returns:
    - list of tuple: All unique combinations of players as tuples, each of length n.
    """
    return [tuple(comb) for comb in combinations(lineup, n)]

@retry
def fetchLineupData(team_id, season, GroupQuantity = '5', dateFrom=None, dateTo=None, measureType="Advanced"):
    """
    Fetch lineup performance data for a given statistic measureType from the NBA Stats API for a
    given team, season, date range, and group configuration.

    This function pulls all lineup combinations of a specified size (e.g., 2–5 players) used by the given team within
    a date range and returns performance statistics. For example, a 2-player grouping will summarize all 5-player
    lineups that include those two players. The lineup statistics are grouped into different measureType categories
    that must be pulled seperatley

    Args:
    - team_id (int): The unique ID of the NBA team.
    - season (str): Season string in the format 'YYYY-YY' (e.g., '2021-22').
    - GroupQuantity (str): Number of players in the lineup group (between '2' and '5').
    - dateFrom (str, optional): Start date for the data in 'MM/DD/YYYY' format. Defaults to None.
    - dateTo (str, optional): End date for the data in 'MM/DD/YYYY' format. Defaults to None.
    - measureType (str): Type of performance statistics to return (e.g., 'Advanced', 'Base', 'Misc').

    Returns:
    - DataFrame: A pandas DataFrame containing the fetched lineup data.
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
    base_url = "https://stats.nba.com/stats/leaguedashlineups"
    params = {
        "Conference": "",
        "DateFrom": dateFrom,
        "DateTo": dateTo,
        "Division": "",
        "GameSegment": "",
        "GroupQuantity": GroupQuantity,
        "LastNGames": "0",
        "LeagueID": "00",
        "Location": "",
        "MeasureType": measureType,
        "Month": "0",
        "OpponentTeamID": "0",
        "Outcome": "",
        "PORound": "0",
        "PaceAdjust": "N",
        "PerMode": "PerMinute",
        "Period": "0",
        "PlusMinus": "N",
        "Rank": "N",
        "Season": season,
        "SeasonSegment": "",
        "SeasonType": "Regular Season",
        "ShotClockRange": "",
        "TeamID": team_id if team_id else "",
        "VsConference": "",
        "VsDivision": "",
    }

    response = requests.get(base_url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()
    headers = data['resultSets'][0]['headers']
    rows = data['resultSets'][0]['rowSet']
    lineupData = pd.DataFrame(rows, columns=headers)

    # Throttle when pulling lots of lineup data so as not to get rate limited by the API
    lag = np.random.uniform(low=0.5,high=1.0)
    time.sleep(lag)

    return lineupData

def fetchAllTeamLineupData(team_id, season, GroupQuantity = '5', minuteThreshold= 12, dateFrom=None, dateTo=None):
    """
    Fetch and consolidate all lineup statistics for a specific team and lineup group across multiple measure types.

    This function retrieves lineup data for the given team and season using the NBA Stats API by calling
    `fetchLineupData` for each of the five measure types: 'Advanced', 'Misc', 'Four Factors', 'Scoring', and 'Opponent'.
    It merges the results into a single DataFrame and filters out lineups that have played
    fewer than the specified number of minutes together.

    Args:
    - team_id (int): The unique ID of the NBA team.
    - season (str): Season string in the format 'YYYY-YY' (e.g., '2021-22').
    - GroupQuantity (str): Number of players in the lineup group (between '2' and '5').
    - minuteThreshold (float): Minimum number of minutes played together to include a lineup (e.g., 12).
    - dateFrom (str, optional): Start date for the data in 'MM/DD/YYYY' format. Defaults to None.
    - dateTo (str, optional): End date for the data in 'MM/DD/YYYY' format. Defaults to None.

    Returns:
    - DataFrame: A consolidated DataFrame of lineup statistics across all measure types.
    """
    measureTypes = ["Advanced", "Misc", "Four Factors", "Scoring", "Opponent"]
    combinedLineupData = None

    # Pull the statistics for all 5 possible different measureType statistic categories and merge them together
    for measure_type in measureTypes:
        lineupData = fetchLineupData(team_id, season,  GroupQuantity, dateFrom, dateTo, measure_type)

        lineupData = lineupData.rename(
            columns=lambda x: f"{measure_type}_{x}" if x not in ['GROUP_ID', 'TEAM_ID'] else x)

        lineupData = lineupData.loc[:, ~lineupData.columns.str.contains('RANK')]

        if combinedLineupData is None:
            combinedLineupData = lineupData
        else:
            combinedLineupData = combinedLineupData.merge(lineupData, on=['GROUP_ID', 'TEAM_ID'], how='left')

    # Filter only for lineups meeting the defined minimum minute threshold
    combinedLineupData = combinedLineupData[combinedLineupData['Advanced_MIN'] >= minuteThreshold]

    combinedLineupData = combinedLineupData[['GROUP_ID','TEAM_ID','Advanced_MIN','Advanced_E_OFF_RATING','Advanced_OFF_RATING',
                       'Advanced_E_DEF_RATING','Advanced_DEF_RATING','Advanced_E_NET_RATING','Advanced_NET_RATING',
                       'Advanced_OREB_PCT','Advanced_DREB_PCT','Advanced_REB_PCT','Advanced_TM_TOV_PCT',
                       'Advanced_EFG_PCT','Advanced_TS_PCT','Advanced_E_PACE','Advanced_PACE',
                       'Misc_PTS_OFF_TOV','Misc_PTS_2ND_CHANCE','Misc_PTS_FB','Misc_PTS_PAINT','Misc_OPP_PTS_OFF_TOV',
                       'Misc_OPP_PTS_2ND_CHANCE','Misc_OPP_PTS_FB','Misc_OPP_PTS_PAINT',
                       'Four Factors_FTA_RATE','Four Factors_OPP_EFG_PCT',
                       'Four Factors_OPP_FTA_RATE','Four Factors_OPP_TOV_PCT','Four Factors_OPP_OREB_PCT',
                       'Scoring_PCT_FGA_2PT','Scoring_PCT_FGA_3PT','Scoring_PCT_PTS_2PT','Scoring_PCT_PTS_2PT_MR',
                       'Scoring_PCT_PTS_3PT','Scoring_PCT_PTS_FB','Scoring_PCT_PTS_FT','Scoring_PCT_PTS_OFF_TOV',
                       'Scoring_PCT_PTS_PAINT','Opponent_OPP_FGM','Opponent_OPP_FGA','Opponent_OPP_FG_PCT',
                       'Opponent_OPP_FG3M','Opponent_OPP_FG3A','Opponent_OPP_FG3_PCT','Opponent_OPP_FTM',
                       'Opponent_OPP_FTA','Opponent_OPP_OREB','Opponent_OPP_DREB','Opponent_OPP_REB']]

    for measureType in measureTypes:
        name_remove = measureType + '_'
        combinedLineupData.columns = combinedLineupData.columns.str.removeprefix(name_remove)

    return combinedLineupData



def loadSeasonLineupData(season, minThreshold):
    """
    Load raw lineup statistics for a given NBA season from the local 'data/lineup_stats' folder.

    The file must follow the naming convention:
    'league_lineups_raw_<season>_minthresh<minThreshold>.pkl'

    Args:
    - season (str): NBA season string in the format 'YYYY-YY' (e.g., '2021-22').
    - minThreshold (int): Minimum minutes played threshold used to filter valid lineups during data preparation.

    Returns:
    - pd.DataFrame: A DataFrame containing lineup statistics for the specified season and threshold.
    """
    base_dir = os.path.dirname(__file__)  # This gets the directory of this script
    filepath = os.path.join(base_dir, '..', 'data', 'lineup_stats', f'league_lineups_raw_{season}_minthresh{minThreshold}.pkl')
    filepath = os.path.abspath(filepath)
    return pd.read_pickle(filepath)

def filterLineupData(lineupData, team_id, groupQuantity, dateFrom, dateTo):
    """
    Filter preloaded lineup statistics for a specific team, player group size, and date range.

    This function narrows down the dataset to rows that match the specified team ID and group size,
    and fall within the provided season-to-date range. Among the matching rows, only the entries with the
    latest available 'DATETO' value are retained.

    Args:
    - lineupData (pd.DataFrame): The full raw season lineup data.
    - team_id (int): Unique identifier for the team.
    - groupQuantity (str): Number of players in the lineup group (e.g., '5', '3').
    - dateFrom (str): Start date in the format '%m/%d/%Y'.
    - dateTo (str): End date in the format '%m/%d/%Y'.

    Returns:
    - pd.DataFrame: Filtered lineup data for the specified team and date range.
    """
    lineupData['DATEFROM'] = pd.to_datetime(lineupData['DATEFROM'], format='%m/%d/%Y')
    lineupData['DATETO'] = pd.to_datetime(lineupData['DATETO'], format='%m/%d/%Y')
    dateFrom = pd.to_datetime(dateFrom, format='%m/%d/%Y')
    dateTo = pd.to_datetime(dateTo, format='%m/%d/%Y')

    filteredData = lineupData[
        (lineupData['TEAM_ID'] == team_id) &
        (lineupData['GroupQuantity'] == groupQuantity) &
        (lineupData['DATEFROM'] == dateFrom) &
        (lineupData['DATETO'] <= dateTo)
        ]

    # Find the maximum DATEFROM in the filtered data
    maxDateTo = filteredData['DATETO'].max()

    # Filter the rows that have this maximum DATEFROM
    finalFilteredData = filteredData[filteredData['DATETO'] == maxDateTo]

    return finalFilteredData


def weightedAvgStatistics(teamSeasonLineupData):
    """
    Calculate weighted average lineup statistics for a team based on minutes played by each lineup in a set of lineups.

    This function aggregates lineup-level statistics into a single row representing the team's
    weighted average performance. The weights are based on the number of minutes each lineup played.

    Args:
    - teamSeasonLineupData (DataFrame): A DataFrame containing lineup statistics for a team.
      Must include a 'MIN' column for weighting and standard NBA lineup stat columns.

    Returns:
    - DataFrame: A single-row DataFrame containing the weighted average statistics.
    """
    def weightedAvg(x):
        return np.average(x, weights=teamSeasonLineupData['MIN'])

    total_minutes = teamSeasonLineupData['MIN'].sum()
    team = teamSeasonLineupData['TEAM_ID'].unique()[0]
    columns_to_drop = ['GROUP_ID', 'TEAM_ID', 'DATEFROM', 'DATETO', 'GroupQuantity', 'MIN']
    columns_to_drop = [col for col in columns_to_drop if col in teamSeasonLineupData.columns]

    weightedData = teamSeasonLineupData.drop(columns=columns_to_drop).apply(weightedAvg)
    weightedData = pd.DataFrame(weightedData).transpose()

    weightedData['GROUP_ID'] = 'Lineup Combo Avg'
    weightedData['MIN'] = total_minutes
    weightedData['TEAM_ID'] = team
    weightedData = weightedData[['GROUP_ID','TEAM_ID','MIN','E_OFF_RATING','OFF_RATING',
                       'E_DEF_RATING','DEF_RATING','E_NET_RATING','NET_RATING',
                       'OREB_PCT','DREB_PCT','REB_PCT','TM_TOV_PCT',
                       'EFG_PCT','TS_PCT','E_PACE','PACE',
                       'PTS_OFF_TOV','PTS_2ND_CHANCE','PTS_FB','PTS_PAINT','OPP_PTS_OFF_TOV',
                       'OPP_PTS_2ND_CHANCE','OPP_PTS_FB','OPP_PTS_PAINT',
                       'FTA_RATE','OPP_EFG_PCT',
                       'OPP_FTA_RATE','OPP_TOV_PCT','OPP_OREB_PCT',
                       'PCT_FGA_2PT','PCT_FGA_3PT','PCT_PTS_2PT','PCT_PTS_2PT_MR',
                       'PCT_PTS_3PT','PCT_PTS_FB','PCT_PTS_FT','PCT_PTS_OFF_TOV',
                       'PCT_PTS_PAINT','OPP_FGM','OPP_FGA','OPP_FG_PCT',
                       'OPP_FG3M','OPP_FG3A','OPP_FG3_PCT','OPP_FTM',
                       'OPP_FTA','OPP_OREB','OPP_DREB','OPP_REB']]

    return weightedData

def extractWeightedLineupGroupStats(lineup, teamSeasonLineupData, groupQuantity, min_minutes_grouping_map):
    """
    Computes weighted average statistics for a lineup based on all valid subgroup combinations.

    This function evaluates all possible lineup combinations of a given group size (e.g., 2–4 players) from
    the 5-player lineup provided. It then filters those subgroups using a minimum minutes threshold and
    returns a weighted average of their statistics based on minutes played. This function is mainly used for
    calculating lineup statistics for a 5-player lineup that has not met the minimum minute threshold.

    Args:
    - lineup (list): List of 5 player IDs representing the lineup.
    - teamSeasonLineupData (DataFrame): Preloaded lineup statistics for the team and season.
    - groupQuantity (int): Number of players in the group (e.g., 2, 3, 4, or 5).
    - min_minutes_grouping_map (dict): Dictionary specifying the minimum minutes threshold for each group size.

    Returns:
    - DataFrame or None: Weighted average statistics for the valid lineup combinations.
    """
    if teamSeasonLineupData is None or teamSeasonLineupData.empty:
        return None

    seasonData = teamSeasonLineupData.copy()

    # Store GROUP_IDs as sorted tuples
    if isinstance(seasonData['GROUP_ID'].iloc[0], str):
        seasonData.loc[:, 'GROUP_ID'] = seasonData['GROUP_ID'].apply(cleanGroupId)

    # Generate all n-player combinations from the current lineup
    lineupCombinations = [tuple(sorted(combo)) for combo in generateLineupCombinations(lineup, groupQuantity)]

    validCombinations = []
    for combo in lineupCombinations:
        stats = seasonData[seasonData['GROUP_ID'] == combo]
        # Keep only groups that exceed the minimum minutes threshold
        if not stats.empty and stats['MIN'].values[0] >= min_minutes_grouping_map[groupQuantity]:
            validCombinations.append(stats)

    # Compute weighted average statistics across all valid combinations
    if validCombinations:
        allValidCombinations = pd.concat(validCombinations)
        return weightedAvgStatistics(allValidCombinations)

    # Return None if no valid combinations found
    return None

def resolveBestAvailableLineupStats(lineup, currentSeasonStatsDict=None, previousSeasonStatsDict=None,
                          fallbackCurrentSeasonStats=None, fallbackPreviousSeasonStats=None):
    """
    Resolves the best available lineup statistics for a given 5-player lineup by checking multiple sources
    in a prioritized order.

    The function first attempts to find group-based lineup statistics from the current season, starting with
    full 5-player groups and falling back to smaller groupings (4, 3, 2). If no valid data is found, it repeats
    the process using previous season stats. If no group-based data is available, it uses fallback team-level
    weighted average statistics.

    Args:
    - lineup (list): List of player IDs (integers) representing the 5-player lineup.
    - currentSeasonStatsDict (dict): Dictionary mapping group sizes ('2'–'5') to DataFrames of current season lineup stats.
    - previousSeasonStatsDict (dict): Dictionary mapping group sizes ('2'–'5') to DataFrames of previous season lineup stats.
    - fallbackCurrentSeasonStats (DataFrame): Team-level current season lineup stats used as a fallback.
    - fallbackPreviousSeasonStats (DataFrame): Team-level previous season lineup stats used as a fallback.

    Returns:
    - DataFrame: A single-row DataFrame containing the resolved lineup statistics.
    """
    # Attempt to extract lineup statistics using current season data
    # Start with full 5-player grouping and fall back to smaller groupings (4, 3, 2)
    for players_n in range(5, 1, -1):
        stats = extractWeightedLineupGroupStats(lineup, currentSeasonStatsDict[str(players_n)], players_n, min_minutes_grouping_map)
        if stats is not None and not stats.empty:
            return stats

    # If no valid stats found in current season, try using previous season data
    for players_n in range(5, 1, -1):
        stats = extractWeightedLineupGroupStats(lineup, previousSeasonStatsDict[str(players_n)], players_n, min_minutes_grouping_map)
        if stats is not None and not stats.empty:
            return stats

    # If no group-based stats are available, fall back to weighted average current season stats for the entire team
    if fallbackCurrentSeasonStats is not None and not fallbackCurrentSeasonStats.empty:
        stats = weightedAvgStatistics(fallbackCurrentSeasonStats)
        return stats

    # If all other sources fail, fall back to  weighted average previous season stats for the entire team
    if fallbackPreviousSeasonStats is not None and not fallbackPreviousSeasonStats.empty:
        stats = weightedAvgStatistics(fallbackPreviousSeasonStats)
        return stats


def calcLineupPercentiles(lineupData):
    """
    Calculates the percentiles for a predefined set of statistics across all lineups in the input data.
    Each statistic is ranked across the dataset using a percentile rank.

    Args:
    - lineupData (DataFrame): DataFrame containing lineup statistics.

    Returns:
    - DataFrame: Original DataFrame with additional columns for percentile ranks of each statistic.
    """
    percentile_columns = ['E_OFF_RATING','OFF_RATING',
                       'E_DEF_RATING','DEF_RATING','E_NET_RATING','NET_RATING',
                       'OREB_PCT','DREB_PCT','REB_PCT','TM_TOV_PCT',
                       'EFG_PCT','TS_PCT','E_PACE','PACE',
                       'PTS_OFF_TOV','PTS_2ND_CHANCE','PTS_FB','PTS_PAINT','OPP_PTS_OFF_TOV',
                       'OPP_PTS_2ND_CHANCE','OPP_PTS_FB','OPP_PTS_PAINT',
                       'FTA_RATE','OREB_PCT','OPP_EFG_PCT',
                       'OPP_FTA_RATE','OPP_TOV_PCT','OPP_OREB_PCT',
                       'PCT_FGA_2PT','PCT_FGA_3PT','PCT_PTS_2PT','PCT_PTS_2PT_MR',
                       'PCT_PTS_3PT','PCT_PTS_FB','PCT_PTS_FT','PCT_PTS_OFF_TOV',
                       'PCT_PTS_PAINT','OPP_FGM','OPP_FGA','OPP_FG_PCT',
                       'OPP_FG3M','OPP_FG3A','OPP_FG3_PCT','OPP_FTM',
                       'OPP_FTA','OPP_OREB','OPP_DREB','OPP_REB']

    for col in percentile_columns:
        lineupData[f'{col}_PERCENTILE'] = lineupData[col].rank(pct=True)

    return lineupData


def calcAvgLineupPercentiles(leagueLineupData, avgLineup):
    """
    Calculates percentile ranks for a given weighted average lineup by comparing it against all 5-player lineups in the
    league.

    Args:
    - leagueLineupData (DataFrame): DataFrame containing statistics for all 5-player lineups.
    - avgLineup (DataFrame): Single-row DataFrame of the weighted average lineup statistics.

    Returns:
    - DataFrame: DataFrame containing percentile ranks, including the average lineup.
    """
    avgLineup = avgLineup.drop("time_window", axis='columns')
    leagueLineupsAndAvglineup = pd.concat([leagueLineupData,avgLineup])

    percentiles = calcLineupPercentiles(leagueLineupsAndAvglineup)

    return percentiles

def percentilesToBins(lineupPercentiles):
    """
    Converts percentile columns into discrete categorical bins for easier interpretation.
    Percentiles are grouped into five ordinal buckets (1 through 5), each representing a 20% range.

    Args:
    - lineupPercentiles (DataFrame): DataFrame containing percentile-ranked statistics.

    Returns:
    - DataFrame: Original DataFrame with new columns mapping each percentile to a discrete bucket.
    """
    percentile_columns = [col for col in lineupPercentiles.columns if '_PERCENTILE' in col]

    for col in percentile_columns:
        lineupPercentiles[f'{col}_BUCKET'] = pd.cut(lineupPercentiles[col], bins=[0, 0.2, 0.4, 0.6, 0.8, 1], labels=[1, 2, 3, 4, 5], include_lowest=True)

    return lineupPercentiles

def generateTimewindowLineupFeatures(pbpData, time_window, seasonLineupData, lastSeasonLineupData):
    """
    Generates lineup-based performance features for each game time window using play-by-play data and preloaded lineup stats.

    For every fixed-length time window in the game, this function computes the weighted average statistics of all lineups
    used by each team during that window. These stats are converted into league-wide percentiles and discretized into
    ordinal categories (1–5). It also computes each team's season-to-date lineup features and attaches them as static
    columns across all windows. Fallback strategies are used if current season data is insufficient.

    Args:
    - pbpData (DataFrame): Play-by-play event log, including lineup and timing information.
    - time_window (int): Size of each game window in seconds (e.g., 120 for 2-minute windows).
    - seasonLineupData (DataFrame): Full lineup stats for the current season.
    - lastSeasonLineupData (DataFrame): Full lineup stats for the previous season.

    Returns:
    - DataFrame: A table where each row corresponds to a time window in the game and includes:
        - Home and away team lineup feature percentiles (binned into 5-level categories)
        - Season-to-date lineup percentile buckets (static across all windows)
    """
    # Assign a time window ID to each event in the play-by-play data
    time_window_column = ((pbpData['eventClock'] - 1) // time_window) + 1
    window_start_column = (time_window_column - 1) * time_window
    window_end_column = time_window_column * time_window

    # Fix edge case where last event is exactly on time boundary
    max_event_time = pbpData['eventClock'].max()
    if max_event_time % time_window == 0:
        time_window_column.loc[pbpData['eventClock'] == max_event_time] = max_event_time // time_window


    # Adjust `window_end` if it exceeds `max_time`
    window_end_column = np.where(window_end_column > max_event_time, max_event_time, window_end_column)

    # Concatenate new columns to game_data
    pbpData = pd.concat([pbpData, time_window_column.rename('time_window'),
                           window_start_column.rename('window_start'),
                           pd.Series(window_end_column, name='window_end')], axis=1)


    pbpData = pbpData[pbpData['HomeOnCourt'].apply(len) == 5]
    pbpData = pbpData[pbpData['AwayOnCourt'].apply(len) == 5]

    # Compute time on court between play-by-play events
    pbpData['time_oncourt'] = pbpData['eventClock'] - pbpData['eventClock'].shift(periods=1,fill_value=0)

    # Initialize dictionaries to store seconds played by each unique lineup per time window
    homeLineup_seconds = defaultdict(lambda: defaultdict(int))
    awayLineup_seconds = defaultdict(lambda: defaultdict(int))

    unique_time_windows = pbpData['time_window'].unique()

    for time_window in unique_time_windows:
        # Filter the DataFrame for the current time window
        df_window = pbpData[pbpData['time_window'] == time_window].copy()

        # Adjust the time_oncourt for the first and last rows to account for overlaps
        if not df_window.empty:
            # Adjust the first row
            first_row = df_window.iloc[0]
            adjusted_time_oncourt_first = first_row['eventClock'] - first_row['window_start']
            df_window.iloc[0, df_window.columns.get_loc('time_oncourt')] = adjusted_time_oncourt_first

            # Adjust the last row
            last_row = df_window.iloc[-1]
            adjustment_last = (last_row['window_end'] - last_row['eventClock']) if last_row['window_end'] <= max_event_time \
                else (max_event_time - last_row['eventClock'])
            adjusted_time_oncourt_last = last_row['time_oncourt'] + adjustment_last
            df_window.iloc[-1, df_window.columns.get_loc('time_oncourt')] = adjusted_time_oncourt_last

        # Aggregate seconds played by each unique home/away lineup in this window
        for index, row in df_window.iterrows():
            homeLineup = tuple(row['HomeOnCourt'])
            awayLineup = tuple(row['AwayOnCourt'])
            homeLineup_seconds[homeLineup][time_window] += row['time_oncourt']
            awayLineup_seconds[awayLineup][time_window] += row['time_oncourt']

    # Convert collected lineup-minute information into DataFrames for home and away teams
    homeresult = []
    awayresult = []
    for lineup, windows in homeLineup_seconds.items():
        for window_start, seconds in windows.items():
            homeresult.append({'HomeOnCourt': lineup, 'time_window': window_start, 'MIN': seconds/60})

    for lineup, windows in awayLineup_seconds.items():
        for window_start, seconds in windows.items():
            awayresult.append({'AwayOnCourt': lineup, 'time_window': window_start, 'MIN': seconds/60})

    awayLineup_windows = pd.DataFrame(awayresult)
    awayLineup_windows['AwayOnCourt'] = awayLineup_windows['AwayOnCourt'].map(list)
    awayLineup_windows = awayLineup_windows[(awayLineup_windows['time_window']!=0)
                                            & (awayLineup_windows['MIN']!=0)].sort_values(by=['time_window'])
    homeLineup_windows = pd.DataFrame(homeresult)
    homeLineup_windows['HomeOnCourt'] = homeLineup_windows['HomeOnCourt'].map(list)
    homeLineup_windows = homeLineup_windows[(homeLineup_windows['time_window']!=0)
                                            & (homeLineup_windows['MIN']!=0)].sort_values(by=['time_window'])

    home_team_id = pbpData['HOME_TEAM_ID'].unique()[0]
    away_team_id = pbpData['AWAY_TEAM_ID'].unique()[0]
    season = pbpData['SEASON'].unique()[0]

    # Extract game date and define season date ranges for filtering lineup data
    game_date = pbpData['GAME_DATE'].unique()[0]

    if season == '2020-21':
        dateFrom = '12/21/2020'
        last_day = '05/16/2021'
        last_day_last_season = '08/14/2020'
        dateFrom_last_season = '10/21/2019'
    if season == '2021-22':
        dateFrom = '10/18/2021'
        last_day = '04/10/2022'
        last_day_last_season = '05/16/2021'
        dateFrom_last_season = '12/21/2020'
    if season == '2022-23':
        dateFrom = '10/17/2022'
        last_day = '04/09/2023'
        last_day_last_season = '04/10/2022'
        dateFrom_last_season = '10/18/2021'
    if season == '2023-24':
        dateFrom = '10/23/2023'
        last_day = '04/14/2024'
        last_day_last_season = '04/09/2023'
        dateFrom_last_season = '10/17/2022'

    # Define the last game day as the cutoff for current season stats
    dateTo = game_date - timedelta(days=1)
    dateTo = datetime.strftime(dateTo, "%m/%d/%Y")

    # Pre-fetch all necessary data for the current and previous seasons, and fallback strategies
    last_season = str(int(season.split('-')[0]) - 1) + '-' + str(season[2]) + str(season[3])
    currentSeasonStatsHome = {}
    previousSeasonStatsHome = {}
    for players in range(5, 1, -1):
        currentSeasonStatsHome[str(players)] = filterLineupData(seasonLineupData, home_team_id, str(players),
                                                                dateFrom, dateTo)
        previousSeasonStatsHome[str(players)] = filterLineupData(lastSeasonLineupData, home_team_id, str(players),
                                                                 dateFrom_last_season, last_day_last_season)

    fallbackCurrentSeasonStatsHome = filterLineupData(seasonLineupData, home_team_id, '5',
                                                      dateFrom, dateTo)
    fallbackPreviousSeasonStatsHome = filterLineupData(lastSeasonLineupData, home_team_id, '5',
                                                       dateFrom_last_season, last_day_last_season)

    currentSeasonStatsAway = {}
    previousSeasonStatsAway = {}
    for players in range(5, 1, -1):
        currentSeasonStatsAway[str(players)] = filterLineupData(seasonLineupData, away_team_id, str(players),
                                                                dateFrom, dateTo)
        previousSeasonStatsAway[str(players)] = filterLineupData(lastSeasonLineupData, away_team_id, str(players),
                                                                 dateFrom_last_season, last_day_last_season)

    fallbackCurrentSeasonStatsAway = filterLineupData(seasonLineupData, away_team_id, '5',
                                                      dateFrom, dateTo)
    fallbackPreviousSeasonStatsAway = filterLineupData(lastSeasonLineupData, away_team_id, '5',
                                                       dateFrom_last_season, last_day_last_season)

    # Loop through each time window and resolve the best available stats for each home lineup
    homeLineupStats = []
    for time_window in homeLineup_windows['time_window'].unique():
        window_data = homeLineup_windows[homeLineup_windows['time_window'] == time_window]
        stats_list = []
        for _, row in window_data.iterrows():
            stats = resolveBestAvailableLineupStats(row['HomeOnCourt'],
                                        currentSeasonStatsHome, previousSeasonStatsHome,
                                        fallbackCurrentSeasonStatsHome, fallbackPreviousSeasonStatsHome)
            if stats is not None and not stats.empty:
                stats['MIN'] = row['MIN']
                stats_list.append(stats)
        if stats_list:
            weighted_stats = weightedAvgStatistics(pd.concat(stats_list))
            weighted_stats['time_window'] = time_window
            homeLineupStats.append(weighted_stats)

    awayLineupStats = []
    for time_window in awayLineup_windows['time_window'].unique():
        window_data = awayLineup_windows[awayLineup_windows['time_window'] == time_window]
        stats_list = []
        for _, row in window_data.iterrows():
            stats = resolveBestAvailableLineupStats(row['AwayOnCourt'],
                                              currentSeasonStatsAway, previousSeasonStatsAway,
                                              fallbackCurrentSeasonStatsAway, fallbackPreviousSeasonStatsAway)
            if stats is not None and not stats.empty:
                stats['MIN'] = row['MIN']
                stats_list.append(stats)
        if stats_list:
            weighted_stats = weightedAvgStatistics(pd.concat(stats_list))
            weighted_stats['time_window'] = time_window
            awayLineupStats.append(weighted_stats)

    # Concatenate time-windowed lineup statistics into final DataFrames
    if homeLineupStats:
        homeLineupStatsDf = pd.concat(homeLineupStats).reset_index(drop=True)
    else:
        homeLineupStatsDf = pd.DataFrame()

    if awayLineupStats:
        awayLineupStatsDf = pd.concat(awayLineupStats).reset_index(drop=True)
    else:
        awayLineupStatsDf = pd.DataFrame()

    # Retrieve league-wide stats for percentile comparisons (fallback to prior season if too few lineups)
    leagueStats = seasonLineupData[(seasonLineupData['GroupQuantity'] == '5')
                                   & (seasonLineupData['DATEFROM'] == dateFrom) & (seasonLineupData['DATETO'] == dateTo)]
    if len(leagueStats.index) < 100:
        leagueStats = lastSeasonLineupData[(lastSeasonLineupData['GroupQuantity'] == '5')
                                           & (lastSeasonLineupData['DATEFROM'] == dateFrom_last_season) &
                                           (lastSeasonLineupData['DATETO'] == last_day_last_season)]


    # Compute percentiles for lineup performance in each time window
    home_percentiles_list = []
    for time_window in homeLineupStatsDf['time_window'].unique():
        window_data = homeLineupStatsDf[homeLineupStatsDf['time_window'] == time_window]
        percentiles = calcAvgLineupPercentiles(leagueStats, window_data)
        percentiles['time_window'] = time_window
        percentiles = percentiles[percentiles['GROUP_ID']=="Lineup Combo Avg"]
        home_percentiles_list.append(percentiles)

    away_percentiles_list = []
    for time_window in awayLineupStatsDf['time_window'].unique():
        window_data = awayLineupStatsDf[awayLineupStatsDf['time_window'] == time_window]
        percentiles = calcAvgLineupPercentiles(leagueStats, window_data)
        percentiles['time_window'] = time_window
        percentiles = percentiles[percentiles['GROUP_ID'] == "Lineup Combo Avg"]
        away_percentiles_list.append(percentiles)

    if home_percentiles_list:
        homePercentilesDf = pd.concat(home_percentiles_list).reset_index(drop=True)
    else:
        homePercentilesDf = pd.DataFrame()

    if away_percentiles_list:
        awayPercentilesDf = pd.concat(away_percentiles_list).reset_index(drop=True)
    else:
        awayPercentilesDf = pd.DataFrame()

    # Load full season lineup data with no minute threshold, used to compute season-to-date statistics for the team
    # These will be static statistics for the team for the entire game that can also be tested if needed in the DBN
    currentSeasonNoMinThresh = loadSeasonLineupData(season, 0.1)
    lastSeasonNoMinThresh = loadSeasonLineupData(last_season, 0.1)
    currentSeasonHomeSeasonTodate = filterLineupData(currentSeasonNoMinThresh, home_team_id, '5', dateFrom, dateTo)
    currentSeasonAwaySeasonTodate = filterLineupData(currentSeasonNoMinThresh, away_team_id, '5', dateFrom, dateTo)

    home_minute_sum = currentSeasonHomeSeasonTodate['MIN'].sum()
    away_minute_sum = currentSeasonAwaySeasonTodate['MIN'].sum()

    if home_minute_sum > 48 * 5 and away_minute_sum > 48 * 5:
        homeSeasonTodateStats = weightedAvgStatistics(currentSeasonHomeSeasonTodate)
        awaySeasonTodateStats = weightedAvgStatistics(currentSeasonAwaySeasonTodate)
        totalLeagueSeasonTodate = currentSeasonNoMinThresh.copy()
        totalLeagueSeasonTodate = totalLeagueSeasonTodate[(totalLeagueSeasonTodate['GroupQuantity'] == '5')
                                                          & (totalLeagueSeasonTodate['DATEFROM'] == dateFrom) &
                                                            (totalLeagueSeasonTodate['DATETO'] <= dateTo)]
        max_dateTo = totalLeagueSeasonTodate['DATETO'].max()
        totalLeagueSeasonTodate = totalLeagueSeasonTodate[totalLeagueSeasonTodate['DATETO'] == max_dateTo]


    else:
        lastSeasonHomeSeasonTodate = filterLineupData(lastSeasonNoMinThresh, home_team_id, '5',
                                                      dateFrom_last_season, last_day_last_season)
        lastSeasonAwaySeasonTodate = filterLineupData(lastSeasonNoMinThresh, away_team_id, '5',
                                                      dateFrom_last_season, last_day_last_season)
        homeSeasonTodateStats = weightedAvgStatistics(lastSeasonHomeSeasonTodate)
        awaySeasonTodateStats = weightedAvgStatistics(lastSeasonAwaySeasonTodate)
        totalLeagueSeasonTodate = lastSeasonNoMinThresh.copy()
        totalLeagueSeasonTodate = totalLeagueSeasonTodate[(totalLeagueSeasonTodate['GroupQuantity'] == '5')
                                                          & (totalLeagueSeasonTodate['DATEFROM'] == dateFrom_last_season) &
                                                            (totalLeagueSeasonTodate['DATETO'] <= last_day_last_season)]
        max_dateTo = totalLeagueSeasonTodate['DATETO'].max()
        totalLeagueSeasonTodate = totalLeagueSeasonTodate[totalLeagueSeasonTodate['DATETO'] == max_dateTo]



    # Map team season-to-date statistics to percentiles. These are static throughout the game
    homeSeasonTodateStats['time_window'] = 0
    awaySeasonTodateStats['time_window'] = 0
    leagueTeamStats = totalLeagueSeasonTodate.groupby('TEAM_ID').apply(weightedAvgStatistics).reset_index(drop=True)
    leagueTeamStats['GROUP_ID'] = 'League_Teams_Avg'
    homeSeasonTodatePercentiles = calcAvgLineupPercentiles(leagueTeamStats, homeSeasonTodateStats)
    homeSeasonTodatePercentiles = homeSeasonTodatePercentiles[homeSeasonTodatePercentiles['GROUP_ID'] == 'Lineup Combo Avg']
    awaySeasonTodatePercentiles = calcAvgLineupPercentiles(leagueTeamStats, awaySeasonTodateStats)
    awaySeasonTodatePercentiles = awaySeasonTodatePercentiles[awaySeasonTodatePercentiles['GROUP_ID'] == 'Lineup Combo Avg']

    homeSeasonTodatePercentiles = homeSeasonTodatePercentiles.loc[:,
                                homeSeasonTodatePercentiles.columns.str.contains('PERCENTILE')]
    awaySeasonTodatePercentiles = awaySeasonTodatePercentiles.loc[:,
                                awaySeasonTodatePercentiles.columns.str.contains('PERCENTILE')]

    # Add these statistics as static columns for each time window for home and away teams
    homeSeasonTodatePercentiles = homeSeasonTodatePercentiles.add_suffix('_HOME_SEASON_TO_DATE')
    awaySeasonTodatePercentiles = awaySeasonTodatePercentiles.add_suffix('_AWAY_SEASON_TO_DATE')

    homeSeasonTodateStatsBuckets = percentilesToBins(homeSeasonTodatePercentiles)
    awaySeasonTodateStatsBuckets = percentilesToBins(awaySeasonTodatePercentiles)

    homeSeasonTodateStatsBuckets = homeSeasonTodateStatsBuckets.loc[:,
                                      homeSeasonTodateStatsBuckets.columns.str.contains('BUCKET')]
    awaySeasonTodateStatsBuckets = awaySeasonTodateStatsBuckets.loc[:,
                                      awaySeasonTodateStatsBuckets.columns.str.contains('BUCKET')]

    homePercentilesDf = percentilesToBins(homePercentilesDf)
    awayPercentilesDf = percentilesToBins(awayPercentilesDf)

    # Merge season-to-date statistics with time window-specific lineup statistics
    if not homePercentilesDf.empty:
        homePercentilesDf = pd.concat([homePercentilesDf,
                                          pd.concat([homeSeasonTodateStatsBuckets] * len(homePercentilesDf),
                                                    ignore_index=True)], axis=1)

    if not awayPercentilesDf.empty:
        awayPercentilesDf = pd.concat([awayPercentilesDf,
                                          pd.concat([awaySeasonTodateStatsBuckets] * len(awayPercentilesDf),
                                                    ignore_index=True)], axis=1)

    combinedLineupStatsDf = pd.merge(homePercentilesDf, awayPercentilesDf, on='time_window',
                                 suffixes=('_home', '_away'))

    combinedLineupStatsDf.drop(['GROUP_ID_home','TEAM_ID_home','MIN_home','DATEFROM_home','DATETO_home','GroupQuantity_home',
                                'GROUP_ID_away','TEAM_ID_away','DATEFROM_away','DATETO_away','GroupQuantity_away',
                                'MIN_away'],axis='columns',inplace=True)

    combinedLineupStatsDf['GAME_ID'] = pbpData['gameid'].unique()[0]
    first_cols = ['GAME_ID','time_window']
    combinedLineupStatsDf = combinedLineupStatsDf[first_cols + [x for x in combinedLineupStatsDf.columns if x not in first_cols]]

    return combinedLineupStatsDf

def generateLineupFeaturesSeason(season,time_window):
    """
    Generates lineup feature data for all games in a given NBA season.

    For each game, this function loads processed play-by-play data and computes time window-level weighted
    average lineup statistics for both teams using current and prior season data. Percentile rankings and
    binned features are computed relative to league-wide distributions. The resulting DataFrame includes one row per
    time window per game with engineered categorical features describing relative lineup strength.

    Args:
    - season (str): The season string (e.g., '2021-22').
    - time_window (int): The length (in seconds) of the time window used to slice play-by-play data.

    Returns:
    - DataFrame: Lineup features for each time interval for all games in the season.
    """
    base_dir = os.path.dirname(__file__)
    open_path = os.path.join(base_dir, '..', 'data', 'pbp_data', f'processed_pbp_{season}.pkl')
    open_path = os.path.abspath(open_path)
    processedSeasonData = pd.read_pickle(open_path)

    seasonLineupData = loadSeasonLineupData(season,.1)

    seasonLineupData = seasonLineupData[seasonLineupData['MIN']>=12]

    last_season = str(int(season.split('-')[0]) - 1) + '-' + str(season[2]) + str(season[3])
    lastSeasonLineupData = loadSeasonLineupData(last_season,.1)
    lastSeasonLineupData = lastSeasonLineupData[lastSeasonLineupData['MIN'] >= 12]

    game_ids = processedSeasonData['gameid'].unique().tolist()
    total_games = len(game_ids)

    seasonData = []
    progress_counter = 0
    with progressbar.ProgressBar(max_value=total_games, widgets=[progressbar.Percentage(), " ",
                                                                 progressbar.GranularBar(), " ",
                                                                 progressbar.AdaptiveETA(), ]) as bar:
        for game in game_ids:
            gameData = processedSeasonData[(processedSeasonData['gameid'] == game)].copy()
            gameData = gameData.sort_values(by=['eventClock', 'orderNumber']).reset_index(drop=True)
            gameData = generateTimewindowLineupFeatures(gameData, time_window, seasonLineupData, lastSeasonLineupData)
            seasonData.append(gameData)
            progress_counter = progress_counter + 1
            bar.update(progress_counter)

    seasonDataframe = pd.concat(seasonData)

    return seasonDataframe

def generateRawLineupStatsSeason(season, start_date, end_date, min_threshold):
    """
    Generates a raw dataset of season-to-date lineup statistics for each team for a season.
    Must provide the start and end dates for the season.

    This function iteratively pulls lineup statistics from the NBA stats API for each team in the league,
    across growing 1 week season-to-date date ranges from `start_date` and `end_date`. For each team and each
    group size (2–5 players), the function retrieves lineup-level performance statistics, filtering out groups
    that do not meet the minimum minutes threshold. Recommend just using 0.1 as the minute threshold.

    Args:
    - season (str): NBA season string (e.g., '2021-22').
    - start_date (str): Start date of the data pull in 'MM/DD/YYYY' format.
    - end_date (str): End date of the data pull in 'MM/DD/YYYY' format.
    - min_threshold (float): Minimum number of minutes a lineup must have played together to be included.

    Returns:
    - DataFrame: A concatenated DataFrame of raw lineup statistics across all teams, weeks, and group sizes.
    """
    rawData = pd.DataFrame()

    seasonSchedule = getSeasonScheduleFrame(season)
    team_ids = seasonSchedule["HOME_TEAM_ID"].unique().tolist()

    # Convert string dates to datetime objects
    start_date = datetime.strptime(start_date, '%m/%d/%Y')
    end_date = datetime.strptime(end_date, '%m/%d/%Y')

    # Calculate the total number of weeks between start and end date
    total_weeks = ((end_date - start_date).days // 7) + 1
    total_iterations = total_weeks * len(team_ids) * 4

    widgets = [progressbar.Percentage(), " ", progressbar.GranularBar(), " ", progressbar.AdaptiveETA(), ]
    pbar = progressbar.ProgressBar(max_value=total_iterations, widgets=widgets)
    pbar.start()
    counter = 0

    # Iterate over each week in the season
    current_date = start_date
    while current_date <= end_date:
        current_date_str = current_date.strftime('%m/%d/%Y')

        # Loop through each team ID. Can't pull all the teams at once due to rate limiting on API
        for team_id in team_ids:
            # Loop through each GroupQuantity (5, 4, 3, 2)
            for group_quantity in ['5', '4', '3', '2']:
                # Call the lineups_statistics function for each team
                lineupStats = fetchAllTeamLineupData(
                    team_id=team_id,
                    season=season,
                    GroupQuantity=group_quantity,
                    minuteThreshold=min_threshold,
                    dateFrom=start_date.strftime('%m/%d/%Y'),
                    dateTo=current_date_str
                )

                # Check if the lineup statistics data is valid
                if not lineupStats.empty and lineupStats.notna().any().any():
                    # Add additional columns for tracking the date range and group quantity
                    lineupStats['DATEFROM'] = start_date.strftime('%m/%d/%Y')
                    lineupStats['DATETO'] = current_date_str
                    lineupStats['GroupQuantity'] = group_quantity
                    first_cols = ['GROUP_ID', 'TEAM_ID', 'DATEFROM', 'DATETO', 'GroupQuantity']
                    lineupStats = lineupStats[first_cols + [x for x in lineupStats.columns if x not in first_cols]]

                    rawData = pd.concat([rawData, lineupStats], ignore_index=True)

                counter += 1
                pbar.update(counter)

        # Move to the next week
        current_date += timedelta(days=7)

    pbar.finish()

    return rawData