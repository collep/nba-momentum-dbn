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
  # only selected features and their lagged versions. Drops rows with mESSing values,
  # converts numeric values to categorical strings, and
  # ensures all features are factors. Appends an 'X' to any column names
  # starting with a number to avoid ESSues in modeling frameworks.
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

  edges = read.csv(full_path, stringsAsFactors = FALSE)
  nodes = names(input_data)

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

bn_cross_validation_holdout = function(input_data, bn_structure, fit_method,
                                       ESS = NULL, runs = 1, random_seed = NULL
                                       ) {
  # ------------------------------------------------------------------------------
  # Perform hold-out cross-validation using a fixed Bayesian network structure
  #
  # This function runs repeated hold-out cross-validation using a pre-specified Bayesian
  # network structure and parameter estimation method.
  #
  # Args:
  # - input_data (data.frame): The input data with all variables as factors.
  # - bn_structure (bn object): A pre-learned Bayesian network structure.
  # - fit_method (str): Parameter fitting method ("mle" or "bayes").
  # - ESS (numeric, optional): Imaginary sample size for Bayesian estimation. Required if `fit_method = "bayes"`.
  # - runs (int): Number of hold-out repetitions. Default is 1.
  # - random_seed (int, optional): Random seed for reproducibility. Default is NULL (no fixed seed).
  #
  # Returns:
  # - bn.cv object: Output from `bn.cv()` containing log-likelihood loss metrics for each run.
  # ------------------------------------------------------------------------------
  if (!is.null(random_seed)) {
    set.seed(random_seed)
  }

    # Validate ESS value if Bayesian fitting is chosen
    if (fit_method == "bayes" && is.null(ESS)) {
      stop("Error: ESS value must be provided when using Bayesian parameter estimation.")
    }

    # Perform cross-validation using the provided structure
    cross_val_results = bn.cv(
      data = input_data,
      bn = bn_structure,  # Pass the structure
      loss = "logl",
      fit = fit_method,
      method = "hold-out",
      runs = runs,
      fit.args = if (fit_method == "bayes") list(ESS = ESS) else list(),
      debug = FALSE
    )

  return(cross_val_results)
}


base_dir = dirname(sys.frame(1)$ofile)
structure_path = file.path(base_dir, "..", "data", "final_network", "final_structure_bnlearn.rds")
structure_path = normalizePath(structure_path)
# Load the saved bnlearn structure object
final_structure_bnlearn = readRDS(structure_path)


base_dir = dirname(sys.frame(1)$ofile)
rdata_path = file.path(base_dir, "..", "data", "input_data", "final_network_unfiltered_input_data.RData")
rdata_path = normalizePath(rdata_path)
load(rdata_path)

feature_csv_filename = "final_feature_list.csv"
input_data = filter_inputdata(structure_learning_unfiltered_input_data_r, feature_csv_filename)
input_data = model_data_asfactors(input_data)

bn_vars = nodes(final_structure_bnlearn)
data_vars = colnames(input_data)
input_data2 = input_data[, bn_vars]

mle_fit = bn_cross_validation_holdout(input_data2, bn_structure = final_structure_bnlearn, fit_method = "mle",
                                      runs = 100, random_seed = 10
                                      )

# bayes_bdeu_ESS1_fit = bn_cross_validation_holdout(input_data2, bn_structure = final_structure_bnlearn,
#                                                 fit_method = "bayes", ESS = 1,
#                                                 runs = 100,  random_seed = 10)
#
# bayes_bdeu_ESS5_fit = bn_cross_validation_holdout(input_data2, bn_structure = final_structure_bnlearn,
#                                                 fit_method = "bayes", ESS = 5,
#                                                 runs = 100,  random_seed = 10)
#
# bayes_bdeu_ESS10_fit = bn_cross_validation_holdout(input_data2, bn_structure = final_structure_bnlearn,
#                                                 fit_method = "bayes", ESS = 10,
#                                                 runs = 100, random_seed = 10)
#
# bayes_bdeu_ESS20_fit = bn_cross_validation_holdout(input_data2, bn_structure = final_structure_bnlearn,
#                                                 fit_method = "bayes", ESS = 20,
#                                                 runs = 100,  random_seed = 10)
#
# bayes_bdeu_ESS50_fit = bn_cross_validation_holdout(input_data2, bn_structure = final_structure_bnlearn,
#                                                 fit_method = "bayes", ESS = 50,
#                                                 runs = 100,  random_seed = 10)
#
# bayes_bdeu_ESS100_fit = bn_cross_validation_holdout(input_data2, bn_structure = final_structure_bnlearn,
#                                                 fit_method = "bayes", ESS = 100,
#                                                 runs = 100, random_seed = 10)