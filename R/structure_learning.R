library(bnlearn)
library(dplyr)
library(tidyr)
library(parallel)
library(openxlsx)

modelDataAsFactors = function(input_data){
  # ------------------------------------------------------------------------------
  # Converts every column in the input data frame to a factor, which is
  # required by structure learning  algorithms in bnlearn.
  #
  # Args:
  # - input_data (data.frame): The input dataset to be prepared for structure learning.
  #
  # Returns:
  # - A data.frame with all columns converted to factors.
  # ------------------------------------------------------------------------------

  data_as_factors = lapply(input_data, as.factor)
  data_as_factors = as.data.frame(data_as_factors)

  data_as_factors= data_as_factors %>% select(where(~ !all(. == .[1])))

  return(data_as_factors)
}

filter_inputdata = function(input_data, feature_csv_filename) {
  # ------------------------------------------------------------------------------
  # Reads a feature configuration CSV and filters the input data to include
  # only selected features and their lagged versions. Drops rows with missing values,
  # converts numeric values to categorical strings, and
  # ensures all features are factors. Appends an 'X' to any column names
  # starting with a number to avoid issues in modeling frameworks.
  #
  # Args:
  # - input_data (data.frame): The input dataset to be prepared for structure learning.
  # - feature_csv_filename (str): The name of the CSV file (located in ../data/input_data/)
  #   that lists features to retain for structure learning.
  #
  # Returns:
  # - data.frame: Filtered and cleaned dataset.
  # ------------------------------------------------------------------------------
  base_dir = dirname(sys.frame(1)$ofile)
  feature_csv_path = file.path(base_dir, "..", "data", "input_data", feature_csv_filename)
  feature_csv_path = normalizePath(feature_csv_path)

  # Read the CSV file with the list of features to use in structure learning
  feature_list = read.csv(feature_csv_path, stringsAsFactors = FALSE)

  # Filter the columns based on 'Feature' status
  features_to_include = feature_list[feature_list$Feature_Exclude == "Feature", "Features"]
  columns_to_include = c()
  for (feature in features_to_include) {
    if (feature %in% colnames(input_data)) {
      columns_to_include = c(columns_to_include, feature)
    }
    lag_columns = grep(paste0("^", feature, ".*lag"), colnames(input_data), value = TRUE)
    columns_to_include = c(columns_to_include, lag_columns)
  }

  columns_to_include = unique(columns_to_include)
  filtered_input_data = input_data[, columns_to_include, drop = FALSE]

  filtered_input_data = na.omit(filtered_input_data)

  filtered_input_data[] = lapply(filtered_input_data, as.character)
  filtered_input_data[] = lapply(filtered_input_data, function(col) {
    if (all(grepl("^[-+]?[0-9]*\\.?[0-9]+$", col))) {
      as.character(as.integer(as.numeric(col)))
    } else {
      col
    }
  })

  filtered_input_data = model_data_asfactors(filtered_input_data)
  # Ensure that features don't start with a numeric character
  colnames(filtered_input_data) = ifelse(grepl("^[0-9]", colnames(filtered_input_data)),
                        paste0("X", colnames(filtered_input_data)),
                        colnames(filtered_input_data))

  return(filtered_input_data)
}

createDynamicBlacklist = function(input_data, numLags) {
  # ------------------------------------------------------------------------------
  # Create a blacklist for structure learning in temporal Bayesian networks
  #
  # Rules enforced by the blacklist:
  # - No edges between different lineup statistics
  # - No edges from game event-drive features to lineup statistics
  # - No edges from a variable in a later time slice to a variable in an earlier time slice
  # - No edges between variables within the same lagged time slice
  #
  # Args:
  # - input_data (data.frame): The full dataset containing variable columns, including lagged variables.
  # - numLags (int): The number of lagged time slices (e.g., 2 for lag1 and lag2).
  #
  # Returns:
  # - A two-column character matrix with 'from' and 'to' columns, representing blacklisted edges.
  # ------------------------------------------------------------------------------

  col_names = names(input_data)

  # Initialize lists to store columns by time slice
  timeslices = vector("list", numLags + 1)
  names(timeslices) = c("current", paste0("lag", 1:numLags))

  # Assign variables to each time slice
  timeslices$current = col_names[!grepl("_lag", col_names)]
  for (i in 1:numLags) {
    timeslices[[paste0("lag", i)]] = col_names[grepl(paste0("_lag", i), col_names)]
  }

  # Initialize the blacklist
  blacklist = matrix(ncol = 2, nrow = 0)
  col_names(blacklist) = c("from", "to")

  # Identify lineup statistic variables (
  lineup_vars = col_names[grepl("BUCKET", col_names)]
  non_lineup_vars = setdiff(col_names, lineup_vars)

  # Blacklist lineup statistic to lineup statistic edges
  if (length(lineup_vars) > 0) {
    for (var1 in lineup_vars) {
      for (var2 in lineup_vars) {
        if (var1 != var2) {  # Prevent self-loops
          blacklist = rbind(blacklist, c(var1, var2))
        }
      }
    }

    # Blacklist edges from game event-driven features to lineup statistics
    if (length(non_lineup_vars) > 0) {
      for (non_lineup in non_lineup_vars) {
        for (lineup_var in lineup_vars) {
          blacklist = rbind(blacklist, c(non_lineup, lineup_var))
        }
      }
    }
  }

  for (i in 1:length(timeslices)) {
    currentSliceName = names(timeslices)[i]
    currentSliceVars = timeslices[[currentSliceName]]

    # Blacklist edges to earlier time slices
    for (j in (i+1):length(timeslices)) {
      if (j > length(timeslices)) {
        break
      }

      earlierSliceVars = timeslices[[j]]

      for (currentVar in currentSliceVars) {
        for (earlierVar in earlierSliceVars) {
          blacklist = rbind(blacklist, c(currentVar, earlierVar))
        }
      }
    }

    # Blacklist edges within lagged slices
    if (currentSliceName != "current") {
      for (var1 in currentSliceVars) {
        for (var2 in currentSliceVars) {
          if (var1 != var2) {
            blacklist = rbind(blacklist, c(var1, var2))
          }
        }
      }
    }
  }

  blacklist = unique(blacklist)

  return(blacklist)
}

create_starting_dag = function(starting_dag_filename,input_data){
  # ------------------------------------------------------------------------------
  # Create a starting DAG from a CSV-defined edge list
  #
  # Args:
  # - starting_dag_filename (str): Filename of the CSV file (located in ../data/input_data/) defining edges
  #   with 'from' and 'to' columns.
  # - input_data (data.frame): Input dataset which defines the nodes of the graph.
  #
  # Returns:
  # - bn DAG object: A bnlearn-compatible DAG initialized with the specified edges.
  # ------------------------------------------------------------------------------
  base_dir = dirname(sys.frame(1)$ofile)
  full_path = file.path(base_dir, "..", "data", "input_data", starting_dag_filename)
  full_path = normalizePath(full_path)

  nodes = names(input_data)
  edges = read.csv(full_path, stringsAsFactors = FALSE)

  edges$from = ifelse(grepl("^[0-9]", edges$from), paste0("X", edges$from), edges$from)
  edges$to = ifelse(grepl("^[0-9]", edges$to), paste0("X", edges$to), edges$to)
  starting_dag = empty.graph(nodes)

  for (i in seq_len(nrow(edges))) {
    edge = edges[i, c("from", "to"), drop = FALSE]

    tryCatch({
      arcs(starting_dag) = rbind(arcs(starting_dag), as.matrix(edge))
    }, error = function(e) {
      cat("Cycle detected or error when adding edge:", edge$from, "->", edge$to, "\n")
      cat("Error message:", e$message, "\n")
      stop("Stopping due to cycle or invalid edge.")
    })
  }

  return(starting_dag)

}

learn_structure = function(input_data, num_lags,  algorithm = "hc", scoring = "aic", starting_dag_filename="empty_dag", tabu = 10, maxtabu = 10){
  # ------------------------------------------------------------------------------
  # Learn a Bayesian network structure using Hill Climbing or Tabu Search
  #
  # Performs structure learning on a given dataset using the specified
  # search algorithm and scoring metric. It supports optional use of a starting DAG
  # and dynamically generates a blacklist to enforce temporal and structural constraints.
  #
  # The function can use either:
  # - Hill Climbing ("hc")
  # - Tabu Search ("tabu")
  #
  # It supports use of:
  # - an empty DAG (default)
  # - a pre-defined starting DAG loaded from CSV
  #
  # Args:
  # - input_data (data.frame): The input data with all variables as factors.
  # - num_lags (int): The number of lagged time slices, used to create the dynamic blacklist.
  # - algorithm (str): Structure learning algorithm ("hc" or "tabu"). Default is "hc".
  # - scoring (str): Scoring metric to evaluate the network ("aic", "bic", "bde", etc.). Default is "aic".
  # - starting_dag_filename (str): Filename of a starting DAG CSV in the data/input_data folder.
  #   Use "empty_dag" to skip. Default is "empty_dag".
  # - tabu (int): Tabu list size for Tabu Search. Only used if algorithm = "tabu". Default is 10.
  # - maxtabu (int): Maximum tabu iterations for Tabu Search. Default is 10.
  #
  # Returns:
  # - A list containing:
  #   - structure: The learned bnlearn DAG object
  #   - structure_score: The overall network score under the selected scoring metric
  # ------------------------------------------------------------------------------
  blacklist = createDynamicBlacklist(input_data,num_lags)

  if(starting_dag_filename!="empty_dag"){
  starting_dag = create_starting_dag(starting_dag_filename,input_data)
  }


  if (algorithm == "hc"){
      if(starting_dag_filename!="empty_dag"){
          bn_structure = hc(input_data, start = starting_dag, score = scoring, blacklist=blacklist )
      }
    if(starting_dag_filename=="empty_dag"){
          bn_structure = hc(input_data, score = scoring, blacklist=blacklist)
      }
  }

  if (algorithm == "tabu"){
    if(starting_dag_filename!="empty_dag"){
          bn_structure = tabu(input_data, start = starting_dag, score = scoring, blacklist = blacklist, tabu = tabu, max.tabu = maxtabu)
    }
    if(starting_dag_filename=="empty_dag"){
          bn_structure = tabu(input_data,  score = scoring, blacklist = blacklist, tabu = tabu, max.tabu = maxtabu)
    }
  }

  network_score = score(bn_structure, input_data, type = scoring)

  if (algorithm == "hc"){
    cat("Hill Climbing ",scoring," Network Score:",network_score, "\n")
  }
  if (algorithm == "tabu"){
    cat("Tabu ",scoring, "with Tabu ",tabu," and MaxTabu ",maxtabu, " Network Score:",network_score, "\n")
  }

  return( list(structure=bn_structure, structure_score=network_score) )

}

struture_learning_stability_analysis = function(input_data, num_lags, algorithm, scoring,
                                      tabu = 10, maxtabu = 10, seed = 100,
                                      num_iterations = 50,
                                      starting_dag_filename = "empty_dag"){
  # ------------------------------------------------------------------------------
  # Perform stability analysis of Bayesian network structure learning
  #
  # Runs multiple iterations of structure learning using a random 60%
  # subsample of the input data in each iteration. Tracks how frequently each edge
  # appears across all learned networks and calculates the prevalence rate.
  #
  # Args:
  # - input_data (data.frame): Full dataset with all features converted to factors.
  # - num_lags (int): Number of lagged time slices used to define the dynamic blacklist.
  # - algorithm (str): Structure learning algorithm ("hc" or "tabu").
  # - scoring (str): Scoring metric to evaluate networks ("aic", "bic", etc.).
  # - tabu (int): Tabu list size (only used if algorithm = "tabu"). Default is 10.
  # - maxtabu (int): Maximum tabu iterations (only used if algorithm = "tabu"). Default is 10.
  # - seed (int): Random seed for reproducibility.
  # - num_iterations (int): Number of random subsampling and structure learning iterations. Default is 50.
  # - starting_dag_filename (str): Filename of a CSV defining the starting DAG, or "empty_dag". Default is "empty_dag".
  #
  # Returns:
  # - A list with two elements:
  #   - edge_summary: A data.frame summarizing edge prevalence across iterations
  #   - network_scores: A data.frame containing the network score for each iteration
  # ------------------------------------------------------------------------------
  edge_counts = list()
  network_scores = data.frame(Iteration = integer(), Algorithm = character(), Score = numeric(), stringsAsFactors = FALSE)

  set.seed(seed)

  for (i in 1:num_iterations) {
    # Randomly sample 60% of the data
    sampled_data = input_data[sample(1:nrow(input_data), size = 0.6 * nrow(input_data)), ]

    if (algorithm == "tabu") {
      result = learn_structure(sampled_data,num_lags = num_lags, algorithm = algorithm, scoring = scoring, starting_dag_filename = starting_dag_filename, tabu = tabu, maxtabu = maxtabu)
    } else {
      result = learn_structure(sampled_data,num_lags = num_lags, algorithm = algorithm, scoring = scoring, starting_dag_filename = starting_dag_filename)
    }

    edges = as.data.frame(result$structure$arcs)
    score = score = result$structure_score

    # Store edges if not empty
    if (nrow(edges) > 0) {
      edges$algorithm = paste0(algorithm, "_", scoring)
      edge_counts[[length(edge_counts) + 1]] = edges
    }

    # Store network score
    network_scores = rbind(network_scores, data.frame(Iteration = i, Algorithm = edges$algorithm[1], Score = score))

    cat("Iteration = ", i, " complete\n")
  }

  all_edges = bind_rows(edge_counts)

  # Compute edge prevalence
  edge_summary = all_edges %>%
    count(from, to, algorithm) %>%
    group_by(from, to, algorithm) %>%
    mutate(prevalence = n / num_iterations) %>%  # Convert raw counts to proportion
    pivot_wider(names_from = algorithm, names_prefix = "prevalence_rate_", values_from = prevalence, values_fill = 0) %>%
    ungroup()

  return( list(edge_summary=edge_summary, network_scores=network_scores) )
}

bn_cross_validation_holdout = function(input_data, num_lags, bn_alg, fit_method, scoring,
                                       starting_dag_filename = "empty_dag",
                                       tabu = 10, maxtabu = 10, runs = 1){
  # ------------------------------------------------------------------------------
  # Perform hold-out cross-validation for Bayesian network structure learning
  #
  # Performs repeated hold-out cross-validation using the bnlearn package.
  # Supports both Hill Climbing ("hc") and Tabu Search ("tabu") algorithms, with optional
  # use of a predefined starting DAG and dynamic blacklisting based on temporal structure.
  # Cross-validation is parallelized across available cores (leaving 2 cores free).
  #
  # Args:
  # - input_data (data.frame): Full dataset with all features converted to factors.
  # - num_lags (int): Number of lagged time slices, used to create the dynamic blacklist.
  # - bn_alg (str): Structure learning algorithm ("hc" or "tabu").
  # - fit_method (str): Parameter estimation method ("mle" or "bayes").
  # - scoring (str): Scoring function used during structure learning ("aic", "bic", "bde", etc.).
  # - starting_dag_filename (str): Name of the CSV file in ../data/input_data/ to use as a
  #   starting DAG, or "empty_dag" for none. Default is "empty_dag".
  # - tabu (int): Tabu list size (only used if bn_alg = "tabu"). Default is 10.
  # - maxtabu (int): Maximum tabu iterations (only used if bn_alg = "tabu"). Default is 10.
  # - runs (int): Number of hold-out repetitions. Default is 1.
  #
  # Returns:
  # - bn.cv object: Output from bnlearn’s `bn.cv()` function containing loss metrics for each run.
  # ------------------------------------------------------------------------------

  blacklist = createDynamicBlacklist(input_data, num_lags)

  if (starting_dag_filename != "empty_dag") {
    starting_dag = create_starting_dag(starting_dag_filename, input_data)
  } else {
    starting_dag = NULL
  }

  # Detect cores and create a cluster for parallel processing
  total_cores = detectCores()
  n_cores = max(1, total_cores - 2)
  cl = makeCluster(n_cores)

  if (bn_alg == "hc") {
    if (is.null(seeded_dag)) {
      cross_val_results = bn.cv(
        input_data,
        bn_alg,
        loss = "logl",
        algorithm.args = list(scoring = scoring, blacklist = blacklist),
        fit = fit_method,
        method = "hold-out",
        runs = runs,
        cluster = cl
      )
    }

    if (!is.null(seeded_dag)) {
      cross_val_results = bn.cv(
        input_data,
        bn_alg,
        loss = "logl",
        algorithm.args = list(start = starting_dag, scoring = scoring, blacklist = blacklist),
        fit = fit_method,
        method = "hold-out",
        runs = runs,
        cluster = cl
      )
    }
  }

  if (bn_alg == "tabu") {
    if (is.null(seeded_dag)) {
      cross_val_results = bn.cv(
        input_data,
        bn_alg,
        loss = "logl",
        algorithm.args = list(scoring = scoring, blacklist = blacklist, tabu = tabu, max.tab = maxtabu),
        fit = fit_method,
        method = "hold-out",
        runs = runs,
        cluster = cl
      )
    }

    if (!is.null(seeded_dag)) {
      cross_val_results = bn.cv(
        input_data,
        bn_alg,
        loss = "logl",
        algorithm.args = list(start = starting_dag, scoring = scoring, blacklist = blacklist, tabu = tabu, max.tab = maxtabu),
        fit = fit_method,
        method = "hold-out",
        runs = runs,
        cluster = cl
      )
    }

  }

  # Stop the cluster
  stopCluster(cl)

  return(cross_val_results)
}

base_dir = dirname(sys.frame(1)$ofile)
rdata_path = file.path(base_dir, "..", "data", "input_data", "final_network_unfiltered_input_data.RData")
rdata_path = normalizePath(rdata_path)
load(rdata_path)

feature_csv_filename = "final_feature_list.csv"
input_data = filter_inputdata(structure_learning_unfiltered_input_data_r, feature_csv_filename)
input_data = model_data_asfactors(input_data)

###HOLDOUT CROSS VALIDATION

bn_cv_hc_aic_mle = bn_cross_validation_holdout(input_data,1,"hc","mle","aic", starting_dag_filename = "starting_dag.csv",runs=10)
# bn_cv_hc_bic_mle = bn_cross_validation_holdout(input_data,1,"hc","mle","bic", starting_dag_filename = "starting_dag.csv",runs=10)
# bn_cv_hc_bde_mle = bn_cross_validation_holdout(input_data,1,"hc","mle","bde", starting_dag_filename = "starting_dag.csv",runs=10)
#
# bn_cv_tabu_25_aic_mle = bn_cross_validation_holdout(input_data,1,"tabu","mle","aic", starting_dag_filename = "starting_dag.csv",tabu=25, maxtabu=25, runs=10)
# bn_cv_tabu_25_bic_mle = bn_cross_validation_holdout(input_data,1,"tabu","mle","bic", starting_dag_filename = "starting_dag.csv",tabu=25, maxtabu=25, runs=10)
# bn_cv_tabu_25_bde_mle = bn_cross_validation_holdout(input_data,1,"tabu","mle","bde", starting_dag_filename = "starting_dag.csv",tabu=25, maxtabu=25, runs=10)
#
# bn_cv_tabu_75_aic_mle = bn_cross_validation_holdout(input_data,1,"tabu","mle","aic", starting_dag_filename = "starting_dag.csv",tabu=75, maxtabu=75, runs=10)
# bn_cv_tabu_75_bic_mle = bn_cross_validation_holdout(input_data,1,"tabu","mle","bic", starting_dag_filename = "starting_dag.csv",tabu=75, maxtabu=75, runs=10)
# bn_cv_tabu_75_bde_mle = bn_cross_validation_holdout(input_data,1,"tabu","mle","bde", starting_dag_filename = "starting_dag.csv",tabu=75, maxtabu=75, runs=10)
#
# bn_cv_tabu_125_aic_mle = bn_cross_validation_holdout(input_data,1,"tabu","mle","aic", starting_dag_filename = "starting_dag.csv",tabu=125, maxtabu=125, runs=10)
# bn_cv_tabu_125_bic_mle = bn_cross_validation_holdout(input_data,1,"tabu","mle","bic", starting_dag_filename = "starting_dag.csv",tabu=125, maxtabu=125, runs=10)
# bn_cv_tabu_125_bde_mle = bn_cross_validation_holdout(input_data,1,"tabu","mle","bde", starting_dag_filename = "starting_dag.csv",tabu=125, maxtabu=125, runs=10)

###Stability Edge Prevalance

stability_hc_aic = struture_learning_stability_analysis(input_data,1,"hc","aic",seed=100,num_iterations = 500, starting_dag_filename = "starting_dag.csv")
#stability_hc_bic = struture_learning_stability_analysis(input_data,1,"hc","bic",seed=100,num_iterations = 500, starting_dag_filename = "starting_dag.csv")
#stability_hc_bde = struture_learning_stability_analysis(input_data,1,"hc","bde",seed=100,num_iterations = 500, starting_dag_filename = "starting_dag.csv")
#stability_tabu_25_aic = struture_learning_stability_analysis(input_data,1,"tabu","aic",seed=100,num_iterations = 500, tabu=25,maxtabu=25,starting_dag_filename = "starting_dag.csv")
#stability_tabu_25_bic = struture_learning_stability_analysis(input_data,1,"tabu","bic",seed=100,num_iterations = 500, tabu=25,maxtabu=25,starting_dag_filename = "starting_dag.csv")
#stability_tabu_25_bde = struture_learning_stability_analysis(input_data,1,"tabu","bde",seed=100,num_iterations = 500, tabu=25,maxtabu=25,starting_dag_filename = "starting_dag.csv")

#stability_tabu_75_aic = struture_learning_stability_analysis(input_data,1,"tabu","aic",seed=100,num_iterations = 500, tabu=75,maxtabu=75,starting_dag_filename = "starting_dag.csv")
#stability_tabu_75_bic = struture_learning_stability_analysis(input_data,1,"tabu","bic",seed=100,num_iterations = 500, tabu=75,maxtabu=75,starting_dag_filename = "starting_dag.csv")
#stability_tabu_75_bde = struture_learning_stability_analysis(input_data,1,"tabu","bde",seed=100,num_iterations = 500, tabu=75,maxtabu=75,starting_dag_filename = "starting_dag.csv")

#stability_tabu_125_aic = struture_learning_stability_analysis(input_data,1,"tabu","aic",seed=100,num_iterations = 500, tabu=125,maxtabu=125,starting_dag_filename = "starting_dag.csv")
#stability_tabu_125_bic = struture_learning_stability_analysis(input_data,1,"tabu","bic",seed=100,num_iterations = 500, tabu=125,maxtabu=125,starting_dag_filename = "starting_dag.csv")
#stability_tabu_125_bde = struture_learning_stability_analysis(input_data,1,"tabu","bde",seed=100,num_iterations = 500, tabu=125,maxtabu=125,starting_dag_filename = "starting_dag.csv")

final_structure = learn_structure(input_data, 1,  algorithm = "hc", scoring = "bic", starting_dag_filename = "starting_dag.csv")