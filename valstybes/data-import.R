# Get the REST API codes for all indicators ###################################

library(request)
library(XML)
library(jsonlite)
library(R.cache)
library(testit)

dataflow_url <- 'https://osp-rs.stat.gov.lt/rest_xml/dataflow/'

get_names_dataflow <- function(
  dataflow_url,
  namespaces = NA
)
{
  # Parses the Dataflow of an SDMX data provider
  
  # :param dataflow_url: URL of the dataflow, that
  # :param namespaces: optional parameter, that specifies the namespaces 
  #       as oppose to letting the function parse them.
  
  # :return: a pair of names and dataflow IDs
  
  dataflow_url_name <- gsub("(https?://)([^/]+)", "\\2", dataflow_url)
  dataflow <- loadCache(key = list(dataflow_url_name))
  
  if(length(dataflow) == 0) {
    print("AAA")
    dataflow <- GET(dataflow_url)
    saveCache(dataflow, key = list(dataflow_url_name))
  }
 
  # Parse the dataflow 
  root <- dataflow$content %>% 
    rawToChar() %>% 
    xmlTreeParse(useInternalNodes = TRUE) %>% 
    xmlRoot()
  
  # Get namespaces, if they weren't given
  namespaces <- xmlNamespaceDefinitions(root, simplify = TRUE)

  # Get dataflow names with XPath
  names <- getNodeSet(root, 
                      path = "//*/com:Name[@xml:lang='en']",
                      namespaces = namespaces)
  
  dataflows <- getNodeSet(root, 
                      path = "//*/str:Dataflow",
                      namespaces = namespaces)

  # Get the values of XML nodes
  list_names <- lapply(names, function(x) xmlValue(x))
  list_ids <- lapply(dataflows, function(x) xmlGetAttr(x, 'id'))
  
  return( list(names=unlist(list_names),
               ids=unlist(list_ids)) )
}

get_indicator_index <- function(
  dataflow,
  indicator_name,
  namespaces = NA
) {
  # Get the names and indices
  ids_names <- get_names_dataflow('https://osp-rs.stat.gov.lt/rest_xml/dataflow/')
  names <- ids_names$names
  ids <- ids_names$ids
  
  # Get the names, that match, and take only the last one
  mask <- grepl(indicator_name, names)
  assert(paste(indicator_name, "does not match any indicator names."),
         sum(mask) > 0)
  
  # Use the mask, obtained from grepl to get the indices. Pick the last one
  matched_ids <- ids[mask]
  last_id <- matched_ids[sum(mask)]
  
  return(last_id)
}

# I will import only the following indicators:
# 1. Fuel and energy resources
# 2. Exports of fuel and energy
# 3. Import of fuel and energy
# 4. Consumer price indices
# 5. Producer price indices

# Parse the XML data ###########################################################

get_indicator <- function(
    data_url,
    index
) {
  sample <- GET('https://osp-rs.stat.gov.lt/rest_xml/data/S1R102_M8020102')$content %>% 
    rawToChar()
  
  sample_xml <- sample %>% 
    xmlTreeParse(useInternalNodes = TRUE) %>% 
    xmlRoot()
  
  namespaces <- xmlNamespaceDefinitions(sample_xml, simplify = TRUE) 
  
  return( list(data=sample_xml, namespaces=namespaces) )
}

parse_observation <- function(
    obs_node,
    dim_names,
    namespaces
)
{
  dimensions <- xpathApply(obs_node,
                           "g:ObsKey/g:Value",
                           namespaces = namespaces)
  
  value <- xpathApply(obs_node,
                      "g:ObsValue",
                      namespaces = namespaces)
  
  dims <- lapply(dimensions, function(x){
    return(xmlGetAttr(x, 'value'))
  })
  
  obs_value <- xmlGetAttr(value[[1]], "value")
  
  output <- c(dims, obs_value)
  names(output) <- dim_names
  
  return( output )
}

get_dimension_names <- function(
    sample_node,
    namespaces
) {
  names <- xpathApply(sample_node,
                      "g:ObsKey/g:Value",
                      namespaces = namespaces)
  
  dim_names <- sapply(names, function(x) {xmlGetAttr(x, "id")})
  
  return(dim_names)
}

get_df_encoded <- function(
    data_url,
    index
) {
  indicator <- get_indicator(data_url, index)
  
  data <- indicator$data
  namespaces <- indicator$namespaces
  
  observations <- xpathApply(sample_xml,
                             "//*/g:Obs",
                             namespaces = namespaces
  )
  
  dims <- get_dimension_names(observations[[1]], namespaces)
  dims <- c(dims, "value")
  
  df <- data.frame(
    do.call("rbind", obs_parsed)) # Need to use do.call as opose to rbind itself
  colnames(df) <- dims
  
  return(df)
}

# TODO: get codelists

get_codelists <- function(x) {}