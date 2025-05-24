# This list defines a comprehensive set of event-driven features derived from
# raw play-by-play logs. Each dictionary specifies the criteria for identifying
# a particular event type (e.g., actionType, subType, qualifiers, descriptors),
# along with associated bin thresholds (based on quantiles over 120-second intervals)
# used to discretize raw event counts into categorical features.
# Further events can be added and/or new versions of categorical bins defined
events_config = [

    ## region REBOUNDS

    {
        "name": "DefensiveRBD",
        "actionType": ["rebound"],
        "subType": ["defensive"],
        "exclude_subType": None,
        "qualifiers": None,
        "exclude_qualifiers": ['deadball'],
        "descriptor": None,
        "exclude_descriptor": None,
        "shotResult": None,
        "assisted": None,
        'steal': None,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [0, 1, 2, 3],
        },

    },

    {
        "name": "OffensiveRBD",
        "actionType": ["rebound"],
        "subType": ["offensive"],
        "exclude_subType": None,
        "qualifiers": None,
        "exclude_qualifiers": ['deadball'],
        "descriptor": None,
        "exclude_descriptor": None,
        "shotResult": None,
        "assisted": None,
        'steal': None,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [0, 1, 2],
        },
    },

    #endregion

    #region 2PT SHOTS

        ##region JUMP SHOTS

    {
        "name": "2p_Jump_Attmpt",
        "actionType": ["2pt"],
        "subType": ["Jump Shot"],
        "exclude_subType": None,
        "qualifiers": None,
        "exclude_qualifiers": None,
        "descriptor": None,
        "exclude_descriptor": None,
        "shotResult": None,
        "assisted": None,
        'steal': None,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [0, 1, 2],
        },
    },

    {
        "name": "2p_Jump_Make",
        "actionType": ["2pt"],
        "subType": ["Jump Shot"],
        "exclude_subType": None,
        "qualifiers": None,
        "exclude_qualifiers": None,
        "descriptor": None,
        "exclude_descriptor": None,
        "shotResult": ["Made"],
        "assisted": None,
        'steal': None,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [0, 1],
        },
    },

    {
        "name": "2p_Midrng_Attmpt",
        "actionType": ["2pt"],
        "subType": None,
        "exclude_subType": None,
        "qualifiers": None,
        "exclude_qualifiers": ["pointsinthepaint"],
        "descriptor": None,
        "exclude_descriptor": None,
        "shotResult": None,
        "assisted": None,
        'steal': None,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [0, 1],
        },
    },

    {
        "name": "2p_Midrng_Make",
        "actionType": ["2pt"],
        "subType": None,
        "exclude_subType": None,
        "qualifiers": None,
        "exclude_qualifiers": ["pointsinthepaint"],
        "descriptor": None,
        "exclude_descriptor": None,
        "shotResult": ["Made"],
        "assisted": None,
        'steal': None,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [0, 1],
        },
    },

        ##endregion

        ##region OVERALL AND QUALIFIERS

    {
        "name": "2p_Attmpt",
        "actionType": ["2pt"],
        "subType": None,
        "exclude_subType": None,
        "qualifiers": None,
        "exclude_qualifiers": None,
        "descriptor": None,
        "exclude_descriptor": None,
        "shotResult": None,
        "assisted": None,
        'steal': None,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [0, 2, 4],

        },
    },

    {
        "name": "2p_Make",
        "actionType": ["2pt"],
        "subType": None,
        "exclude_subType": None,
        "qualifiers": None,
        "exclude_qualifiers": None,
        "descriptor": None,
        "exclude_descriptor": None,
        "shotResult": ["Made"],
        "assisted": None,
        'steal': None,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [0, 1, 2],
        },
    },

    {
        "name": "2p_Paint_Attmpt",
        "actionType": ["2pt"],
        "subType": None,
        "exclude_subType": None,
        "qualifiers": ['pointsinthepaint'],
        "exclude_qualifiers": None,
        "descriptor": None,
        "exclude_descriptor": None,
        "shotResult": None,
        "assisted": None,
        'steal': None,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [0, 1, 2, 3],
        },
    },

    {
        "name": "2p_Paint_Make",
        "actionType": ["2pt"],
        "subType": None,
        "exclude_subType": None,
        "qualifiers": ['pointsinthepaint'],
        "exclude_qualifiers": None,
        "descriptor": None,
        "exclude_descriptor": None,
        "shotResult": ["Made"],
        "assisted": None,
        'steal': None,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [0, 1, 2],
        },
    },

        ##endregion

    #endregion

    #region 3PT SHOTS

        ##region OVERALL AND QUALIFIERS

    {
        "name": "3p_Attmpt",
        "actionType": ["3pt"],
        "subType": None,
        "exclude_subType": None,
        "qualifiers": None,
        "exclude_qualifiers": None,
        "descriptor": None,
        "exclude_descriptor": None,
        "shotResult": None,
        "assisted": None,
        'steal': None,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [0, 1, 2, 3],
        },
    },

    {
        "name": "3p_Make",
        "actionType": ["3pt"],
        "subType": None,
        "exclude_subType": None,
        "qualifiers": None,
        "exclude_qualifiers": None,
        "descriptor": None,
        "exclude_descriptor": None,
        "shotResult": ["Made"],
        "assisted": None,
        'steal': None,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [0, 1],
        },
    },

        ##endregion

    #endregion

    #region 2PT/3PT OVERALL AND QUALIFIERS

    {
        "name": "Shot_Attmpt",
        "actionType": ["2pt","3pt"],
        "subType": None,
        "exclude_subType": None,
        "qualifiers": None,
        "exclude_qualifiers": None,
        "descriptor": None,
        "exclude_descriptor": None,
        "shotResult": None,
        "assisted": None,
        'steal': None,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [0, 3, 4, 5],
        },
    },

    {
        "name": "Shot_Make",
        "actionType": ["2pt","3pt"],
        "subType": None,
        "exclude_subType": None,
        "qualifiers": None,
        "exclude_qualifiers": None,
        "descriptor": None,
        "exclude_descriptor": None,
        "shotResult": ["Made"],
        "assisted": None,
        'steal': None,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [0, 1, 2, 3],
        },
    },

    {
        "name": "Shot_Make_Assisted",
        "actionType": ["2pt","3pt"],
        "subType": None,
        "exclude_subType": None,
        "qualifiers": None,
        "exclude_qualifiers": None,
        "descriptor": None,
        "exclude_descriptor": None,
        "shotResult": ["Made"],
        "assisted": True,
        'steal': None,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [0, 1, 2],
        },
    },

    {
        "name": "Shot_Make_UnAssisted",
        "actionType": ["2pt","3pt"],
        "subType": None,
        "exclude_subType": None,
        "qualifiers": None,
        "exclude_qualifiers": None,
        "descriptor": None,
        "exclude_descriptor": None,
        "shotResult": ["Made"],
        "assisted": False,
        'steal': None,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [0, 1, 2],
        },
    },

    {
        "name": "Shot_2ndChnc_Attmpt",
        "actionType": ["2pt", "3pt"],
        "subType": None,
        "exclude_subType": None,
        "qualifiers": ["2ndchance"],
        "exclude_qualifiers": None,
        "descriptor": None,
        "exclude_descriptor": None,
        "shotResult": None,
        "assisted": None,
        'steal': None,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [0, 1],
        },
    },

    {
        "name": "Shot_2ndChnc_Make",
        "actionType": ["2pt", "3pt"],
        "subType": None,
        "exclude_subType": None,
        "qualifiers": ["2ndchance"],
        "exclude_qualifiers": None,
        "descriptor": None,
        "exclude_descriptor": None,
        "shotResult": ["Made"],
        "assisted": None,
        'steal': None,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [0, 1],
        },
    },

    {
        "name": "Shot_FstBrk_Attmpt",
        "actionType": ["2pt", "3pt"],
        "subType": None,
        "exclude_subType": None,
        "qualifiers": ["fastbreak"],
        "exclude_qualifiers": None,
        "descriptor": None,
        "exclude_descriptor": None,
        "shotResult": None,
        "assisted": None,
        'steal': None,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [0, 1],
        },
    },

    {
        "name": "Shot_FstBrk_Make",
        "actionType": ["2pt", "3pt"],
        "subType": None,
        "exclude_subType": None,
        "qualifiers": ["fastbreak"],
        "exclude_qualifiers": None,
        "descriptor": None,
        "exclude_descriptor": None,
        "shotResult": ["Made"],
        "assisted": None,
        'steal': None,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [0, 1],
        },
    },

    {
        "name": "Shot_FrmTurn_Attmpt",
        "actionType": ["2pt", "3pt"],
        "subType": None,
        "exclude_subType": None,
        "qualifiers": ["fromturnover"],
        "exclude_qualifiers": None,
        "descriptor": None,
        "exclude_descriptor": None,
        "shotResult": None,
        "assisted": None,
        'steal': None,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [0, 2],
        },
    },

    {
        "name": "Shot_FrmTurn_Make",
        "actionType": ["2pt", "3pt"],
        "subType": None,
        "exclude_subType": None,
        "qualifiers": ["fromturnover"],
        "exclude_qualifiers": None,
        "descriptor": None,
        "exclude_descriptor": None,
        "shotResult": ["Made"],
        "assisted": None,
        'steal': None,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [0, 1],
        },
    },

    #endregion

    #region FOULS

    {
        "name": "Off_Foul",
        "actionType": ["foul"],
        "subType": ["offensive"],
        "exclude_subType": None,
        "qualifiers": None,
        "exclude_qualifiers": None,
        "descriptor": None,
        "exclude_descriptor": None,
        "shotResult": None,
        "assisted": None,
        'steal': None,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [1],
        },
    },

    {
        "name": "Off_Charge_Foul",
        "actionType": ["foul"],
        "subType": ["offensive"],
        "exclude_subType": None,
        "qualifiers": None,
        "exclude_qualifiers": None,
        "descriptor": ["charge"],
        "exclude_descriptor": None,
        "shotResult": None,
        "assisted": None,
        'steal': None,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [1],
        },
    },

    {
        "name": "Pers_Foul",
        "actionType": ["foul"],
        "subType": ["personal"],
        "exclude_subType": None,
        "qualifiers": None,
        "exclude_qualifiers": None,
        "descriptor": None,
        "exclude_descriptor": ["flagrant-type-1","flagrant-type-2"],
        "shotResult": None,
        "assisted": None,
        'steal': None,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [0, 1, 2],
        },
    },

    {
        "name": "Pers_InPenalty_Foul",
        "actionType": ["foul"],
        "subType": ["personal"],
        "exclude_subType": None,
        "qualifiers": ['inpenalty'],
        "exclude_qualifiers": None,
        "descriptor": None,
        "exclude_descriptor": None,
        "shotResult": None,
        "assisted": None,
        'steal': None,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [0, 1],
        },
    },

    {
        "name": "Pers_InPenalty_NonShooting_Foul",
        "actionType": ["foul"],
        "subType": ["personal"],
        "exclude_subType": None,
        "qualifiers": ['inpenalty'],
        "exclude_qualifiers": None,
        "descriptor": None,
        "exclude_descriptor": ["shooting"],
        "shotResult": None,
        "assisted": None,
        'steal': None,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [0, 1],
        },
    },

    {
        "name": "Pers_Shooting_Foul",
        "actionType": ["foul"],
        "subType": ["personal"],
        "exclude_subType": None,
        "qualifiers": None,
        "exclude_qualifiers": None,
        "descriptor": ["shooting"],
        "exclude_descriptor": None,
        "shotResult": None,
        "assisted": None,
        'steal': None,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [0, 1],
        },
    },

    {
        "name": "Pers_Shooting_And1_Foul",
        "actionType": ["foul"],
        "subType": ["personal"],
        "exclude_subType": None,
        "qualifiers": ['1freethrow'],
        "exclude_qualifiers": None,
        "descriptor": ["shooting"],
        "exclude_descriptor": None,
        "shotResult": None,
        "assisted": None,
        'steal': None,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [1],
        },
    },

    {
        "name": "Flagrant_Foul",
        "actionType": ["foul"],
        "subType": ["personal"],
        "exclude_subType": None,
        "qualifiers": None,
        "exclude_qualifiers": None,
        "descriptor": ["flagrant-type-1", "flagrant-type-2"],
        "exclude_descriptor": None,
        "shotResult": None,
        "assisted": None,
        'steal': None,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [1],
        },
    },

    {
        "name": "Technical_Foul",
        "actionType": ["foul"],
        "subType": ["technical"],
        "exclude_subType": None,
        "qualifiers": None,
        "exclude_qualifiers": None,
        "descriptor": None,
        "exclude_descriptor": None,
        "shotResult": None,
        "assisted": None,
        'steal': None,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [1],

        },
    },

    #endregion

    #region FREE THROWS

    {
        "name": "FT_Attempt",
        "actionType": ["freethrow"],
        "subType": ["1 of 2","2 of 2","1 of 1","1 of 3","2 of 3","3 of 3","shot"],
        "exclude_subType": None,
        "qualifiers": None,
        "exclude_qualifiers": None,
        "descriptor": None,
        "exclude_descriptor": None,
        "shotResult": None,
        "assisted": None,
        'steal': None,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [0, 1, 2],
        },
    },

    {
        "name": "FT_Make",
        "actionType": ["freethrow"],
        "subType": ["1 of 2", "2 of 2", "1 of 1", "1 of 3", "2 of 3", "3 of 3", "shot"],
        "exclude_subType": None,
        "qualifiers": None,
        "exclude_qualifiers": None,
        "descriptor": None,
        "exclude_descriptor": None,
        "shotResult": ["Made"],
        "assisted": None,
        'steal': None,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [0, 2],
        },
    },


    #endregion

    #region STOPPAGE

    {
        "name": "Instant_Replay",
        "actionType": ["instantreplay"],
        "subType": None,
        "exclude_subType": None,
        "qualifiers": None,
        "exclude_qualifiers": None,
        "descriptor": None,
        "exclude_descriptor": None,
        "shotResult": None,
        "assisted": None,
        'steal': None,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [1],
        },
    },

    {
        "name": "Ejection",
        "actionType": ["ejection"],
        "subType": None,
        "exclude_subType": None,
        "qualifiers": None,
        "exclude_qualifiers": None,
        "descriptor": None,
        "exclude_descriptor": None,
        "shotResult": None,
        "assisted": None,
        'steal': None,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [1],
        },
    },

    {
        "name": "Stoppage",
        "actionType": ["stoppage"],
        "subType": None,
        "exclude_subType": ["out-of-bounds"],
        "qualifiers": None,
        "exclude_qualifiers": None,
        "descriptor": None,
        "exclude_descriptor": None,
        "shotResult": None,
        "assisted": None,
        'steal': None,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [1],
        },
    },

    {
        "name": "Timeout_Team",
        "actionType": ["timeout"],
        "subType": None,
        "exclude_subType": None,
        "qualifiers": ['team'],
        "exclude_qualifiers": None,
        "descriptor": None,
        "exclude_descriptor": None,
        "shotResult": None,
        "assisted": None,
        'steal': None,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [0, 1],
        },
    },

    {
        "name": "Timeout_Mandatory",
        "actionType": ["timeout"],
        "subType": None,
        "exclude_subType": None,
        "qualifiers": ['mandatory'],
        "exclude_qualifiers": None,
        "descriptor": None,
        "exclude_descriptor": None,
        "shotResult": None,
        "assisted": None,
        'steal': None,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [0, 1],
        },
    },

    {
        "name": "EndQuarter",
        "actionType": ["period"],
        "subType": ["end"],
        "exclude_subType": None,
        "qualifiers": None,
        "exclude_qualifiers": None,
        "descriptor": None,
        "exclude_descriptor": None,
        "shotResult": None,
        "assisted": None,
        'steal': None,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [1],
        },
    },

    #endregion

    #region TURNOVER

    {
        "name": "Turnover",
        "actionType": ["turnover"],
        "subType": None,
        "exclude_subType": None,
        "qualifiers": None,
        "exclude_qualifiers": None,
        "descriptor": None,
        "exclude_descriptor": None,
        "shotResult": None,
        "assisted": None,
        'steal': None,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [0, 1, 2],
        },
    },

    {
        "name": "Turnover_ShotClock",
        "actionType": ["turnover"],
        "subType": ["shot clock"],
        "exclude_subType": None,
        "qualifiers": None,
        "exclude_qualifiers": None,
        "descriptor": None,
        "exclude_descriptor": None,
        "shotResult": None,
        "assisted": None,
        'steal': None,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [1],
        },
    },

    #endregion

    #region SUBSTITUTIONS

    {
        "name": "Substitution",
        "actionType": ["substitution"],
        "subType": ['in'],
        "exclude_subType": None,
        "qualifiers": None,
        "exclude_qualifiers": None,
        "descriptor": None,
        "exclude_descriptor": None,
        "shotResult": None,
        "assisted": None,
        'steal': None,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [0, 1, 3],
        },
    },

    #endregion

    #region MISC

    {
        "name": "offense_poss",
        "actionType": None,
        "subType": None,
        "exclude_subType": None,
        "qualifiers": None,
        "exclude_qualifiers": None,
        "descriptor": None,
        "exclude_descriptor": None,
        "shotResult": None,
        "assisted": None,
        'steal': None,
        'block': None,
        'new_offense_poss': True,
        "bins": {
            "bin_120_quantile_1": [0, 4, 5],
        },
    },

    {
        "name": "Block",
        "actionType": None,
        "subType": None,
        "exclude_subType": None,
        "qualifiers": None,
        "exclude_qualifiers": None,
        "descriptor": None,
        "exclude_descriptor": None,
        "shotResult": None,
        "assisted": None,
        'FT_RBD_opp': None,
        'steal': None,
        'block': True,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [0, 1],
        },
    },

{
        "name": "Steal",
        "actionType": None,
        "subType": None,
        "exclude_subType": None,
        "qualifiers": None,
        "exclude_qualifiers": None,
        "descriptor": None,
        "exclude_descriptor": None,
        "shotResult": None,
        "assisted": None,
        'FT_RBD_opp': None,
        'steal': True,
        'block': None,
        'new_offense_poss': None,
        "bins": {
            "bin_120_quantile_1": [0, 1],
            "bin_120_quantile_2": [0, 1],
        },
    },

    #endregion

]

# A filtered list of event feature names (excluding 'overall' game events) that
# are intended to be summed separately for home and away teams over each
# 120-second interval. These variables form the team-level event count features.
all_event_columns = [event['name'] for event in events_config]
overall_events = ['EndQuarter','Timeout_Mandatory','Stoppage','Instant_Replay']
event_columns_to_sum_teams = [event for event in all_event_columns if event not in overall_events]


# This list defines calculated features that combine pairs of base event variables
# (e.g., 2pt_Make / offense_poss) to create interpretable standardized metrics.
# Each engineered feature includes quantile-based bin thresholds used to discretize
# the resulting continuous ratio into ordinal categories for use in the DBN.
# Further events can be added and/or new versions of categorical bins defined
engineered_features = [

    {'name': '2pt_Makes_PerP',
     'calc': ('2p_Make','offense_poss'),
     'bins': {
         'bin_120_quantile_1': [0,0.2,1/3,2/3],
     }
     },
    {'name': '2pt_Efficiency',
     'calc': ('2p_Make','2p_Attmpt'),
     'bins': {
         'bin_120_quantile_1': [0,1/3,2/3,1],
     }
     },
    {'name': '3pt_Makes_PerP',
     'calc': ('3p_Make','offense_poss'),
     'bins': {
         'bin_120_quantile_1': [0, 0.2, 1/3],
     }
     },
    {'name': '3pt_Efficiency',
     'calc': ('3p_Make', '3p_Attmpt'),
     'bins': {
         'bin_120_quantile_1': [0,1/3,2/3,1],
     }
     },
    {'name': 'Shooting_Efficiency',
     'calc': ('Shot_Make','Shot_Attmpt'),
     'bins': {
         'bin_120_quantile_1': [0,1/3,1/2,2/3,1],
     }
     },

    {'name': '2pt_MidRng_Make_PerP',
     'calc': ('2p_Midrng_Make', 'offense_poss'),
     'bins': {
         'bin_120_quantile_1': [0, 0.25],
     }
     },
    {'name': '2pt_Paint_Rate',
     'calc': ('2p_Paint_Attmpt','2p_Attmpt'),
     'bins': {
         'bin_120_quantile_1': [0, 2/3, 1],
     }
     },
    {'name': '2pt_MidRng_Rate',
     'calc': ('2p_Midrng_Attmpt','2p_Attmpt'),
     'bins': {
         'bin_120_quantile_1': [0, 1/2],
     }
     },

    {'name': '2pt_Paint_PerP',
     'calc': ('2p_Paint_Attmpt', 'offense_poss'),
     'bins': {
         'bin_120_quantile_1': [0, 0.25, 0.5, 0.8],
     }
     },
    {'name': '2pt_MidRng_PerP',
     'calc': ('2p_Midrng_Attmpt', 'offense_poss'),
     'bins': {
         'bin_120_quantile_1': [0, 1/3],
     }
     },

    {'name': '2pt_Paint_Make_PerP',
     'calc': ('2p_Paint_Make', 'offense_poss'),
     'bins': {
         'bin_120_quantile_1': [0, 0.25, 0.5],
     }
     },
    {'name': 'FastBreak_Rate',
     'calc': ('Shot_FstBrk_Attmpt','Shot_Attmpt'),
     'bins': {
         'bin_120_quantile_1': [0, 1/3],
     }
     },

    {'name': 'FastBreak_PerP',
     'calc': ('Shot_FstBrk_Attmpt', 'offense_poss'),
     'bins': {
         'bin_120_quantile_1': [0, 0.25],
     }
     },
    {'name': 'FastBreak_DReb_Rate',
     'calc': ('Shot_FstBrk_Attmpt', 'DefensiveRBD'),
     'bins': {
         'bin_120_quantile_1': [0, 1],
     }
     },
    {'name': 'FastBreak_Make_PerP',
     'calc': ('Shot_FstBrk_Make', 'offense_poss'),
     'bins': {
         'bin_120_quantile_1': [0, 0.25],
     }
     },
    {'name': '2pt_Rate',
     'calc': ('2p_Attmpt','Shot_Attmpt'),
     'bins': {
         'bin_120_quantile_1': [0, 0.25, 0.5, 0.75],
     }
     },
    {'name': '3pt_Rate',
     'calc': ('3p_Attmpt','Shot_Attmpt'),
     'bins': {
         'bin_120_quantile_1': [0, 0.25, 0.5, 0.75],
     }
     },

    {'name': '2pt_PerP',
     'calc': ('2p_Attmpt','offense_poss'),
     'bins': {
         'bin_120_quantile_1': [0, 0.25, 0.4, 0.75],
     }
     },
    {'name': '3pt_PerP',
     'calc': ('3p_Attmpt','offense_poss'),
     'bins': {
         'bin_120_quantile_1': [0, 0.25, 0.4, 0.75],
     }
     },

    {'name': 'Turnover_PerP',
     'calc': ('Turnover','offense_poss'),
     'bins': {
         'bin_120_quantile_1': [0, 0.2, 0.4],
     }
     },
    {'name': 'Shot_FrmTurn_PerP',
     'calc': ('Shot_FrmTurn_Attmpt', 'offense_poss'),
     'bins': {
         'bin_120_quantile_1': [0, 1/3],
     }
     },
    {'name': 'Shot_FrmTurn_Make_PerP',
     'calc': ('Shot_FrmTurn_Make', 'offense_poss'),
     'bins': {
         'bin_120_quantile_1': [0, 0.25],
     }
     },
    {'name': 'Shot_2ndChnc_PerP',
     'calc': ("Shot_2ndChnc_Attmpt", 'offense_poss'),
     'bins': {
         'bin_120_quantile_1': [0, 1/3],
     }
     },
    {'name': 'Shot_2ndChnc_Make_PerP',
     'calc': ("Shot_2ndChnc_Make", 'offense_poss'),
     'bins': {
         'bin_120_quantile_1': [0, 0.25],
     }
     },

    {'name': 'Shots_PerP',
     'calc': ('Shot_Attmpt','offense_poss'),
     'bins': {
         'bin_120_quantile_1': [0, 0.75, 1, 4/3],
     }
     },

    {'name': 'Shot_Make_PerP',
     'calc': ('Shot_Make', 'offense_poss'),
     'bins': {
         'bin_120_quantile_1': [0, 0.25, 0.5, 0.75],
     }
     },

    {'name': 'Shot_Make_Assisted_Rate',
     'calc': ("Shot_Make_Assisted", 'Shot_Make'),
     'bins': {
         'bin_120_quantile_1': [0, 0.25, 2/3, 1],
     }
     },

    {'name': 'FT_Attmpt_PerP',
     'calc': ("FT_Attempt", 'offense_poss'),
     'bins': {
         'bin_120_quantile_1': [0, 0.2, 2/3],
     }
     },

    {'name': 'FT_Make_PerP',
     'calc': ("FT_Make", 'offense_poss'),
     'bins': {
         'bin_120_quantile_1': [0, 0.5],
     }
     },
    {'name': 'FT_Efficiency',
     'calc': ("FT_Make", "FT_Attempt"),
     'bins': {
         'bin_120_quantile_1': [0,1],
     }
     },


]

# Defines binning strategies for extra engineered features that are created
# dynamically within other functions in dbn_input_data_processing.py
additional_categorical_bins = [
    {'name': "DReb_Rate_home",
     'bins': {
         'bin_120_quantile_1': [0, 0.5, 1],
    }
    },
    {'name': "DReb_Rate_away",
     'bins': {
         'bin_120_quantile_1': [0, 0.5, 1],
     }
     },
    {'name': "OReb_Rate_home",
     'bins': {
         'bin_120_quantile_1': [0, 0.5, 1],
     }
     },
    {'name': "OReb_Rate_away",
     'bins': {
         'bin_120_quantile_1': [0, 0.5, 1],
     }
     },
    {'name': 'FastBreak_PerOppMiss_away',
     'bins': {
         'bin_120_quantile_1': [0, 2/3],
     }
     },
    {'name': 'FastBreak_PerOppMiss_home',
     'bins': {
         'bin_120_quantile_1': [0, 2/3],
     }
     },
    {'name': 'Missed_Shots_home',
     'bins': {
         'bin_120_quantile_1': [0,1,2,4],
     }
     },
    {'name': 'Missed_Shots_away',
     'bins': {
         'bin_120_quantile_1': [0,1,2,4],
     }
     },


]
