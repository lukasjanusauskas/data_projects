# Get the REST API codes for all indicators ###################################

library(request)
library(XML)
library(jsonlite)
library(R.cache)
library(testit)
library(hash)

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
)
{
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
  xml_data <- GET(paste0(data_url, 'data/', index))$content %>% 
    rawToChar() %>% 
    xmlTreeParse(useInternalNodes = TRUE) %>% 
    xmlRoot()
  
  namespaces <- xmlNamespaceDefinitions(xml_data, simplify = TRUE) 
  
  struct_code <- xpathApply(
    xml_data, "//*/mes:DataSet", namespaces = namespaces
  )
  struct_code <- xmlGetAttr(struct_code[[1]], "structureRef")
  struct_url <- paste0(data_url, 'datastructure/LSD/', struct_code)
  
  return( list(data=xml_data, namespaces=namespaces, struct_url=struct_url) )
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
)
{
  names <- xpathApply(sample_node,
                      "g:ObsKey/g:Value",
                      namespaces = namespaces)
  
  dim_names <- sapply(names, function(x) {xmlGetAttr(x, "id")})
  
  return(dim_names)
}


get_codelists <- function(
    xml_data, 
    struct_url, 
    dim_names
)
{
  codelists <- hash()
  
  xml_meta <- GET(struct_url)$content %>% 
    rawToChar() %>% 
    xmlTreeParse(useInternalNodes = TRUE) %>% 
    xmlRoot()
  
  struct_ns <- xmlNamespaceDefinitions(xml_meta, simplify = TRUE)
  
  for(dim_name in dim_names) {
    codelists[[dim_name]] <- hash()
    
    xpath <- paste0( "//*/str:Codelist[@id='", dim_name, "']/str:Code")
    codes <- xpathApply(xml_meta, xpath,
                          namespaces = struct_ns)
    
    for(code in codes) {
      code_name <- xmlGetAttr(code, "id")
      
      code_value <- xpathApply(code, 
                               "com:Name[@xml:lang='en']", 
                               namespaces = struct_ns)

      codelists[[dim_name]][[code_name]] <- xmlValue(code_value)
    }
  }
  
  return (codelists)
}

get_dataframe <- function(
    data_url,
    index
) {
  dataflow_url_name <- gsub("(https?://)([^/]+)", "\\2", data_url)
  data_url_name <- paste0(dataflow_url_name, index)
  key_list <- list(data_url_name)
  
  # If the data set is already in R cache, use that
  data <- loadCache(key=key_list)
  if(len(data) != 0) {
    return (data) 
  }
  
  # Import the indicator
  indicator <- get_indicator(data_url, index)
  
  data <- indicator$data
  namespaces <- indicator$namespaces
  struct_url <- indicator$struct_url
  
  # Get observations and their names
  observations <- xpathApply(data,
                             "//*/g:Obs",
                             namespaces = namespaces
  )
  
  dims <- get_dimension_names(observations[[1]], namespaces)
  dims <- c(dims, "value")
  
  codelists <- get_codelists(data, 
                             struct_url, 
                             dims[1:length(dims)-1])
  
  # Parse observations
  obs_parsed <- lapply(observations, 
                  function(x) parse_observation(x, dims, namespaces))
  
  df <- data.frame(
    do.call("rbind", obs_parsed)) # Need to use do.call as oppose to rbind itself
  colnames(df) <- dims
  
  for(dim in dims) {
    # If there is no provided code lists, skip
    if( length(codelists[[dim]]) == 0) { next }
    # Map the values, using the hashmap
    df[[dim]] <- sapply( df[[dim]], function(x) codelists[[dim]][[x]] )
  }
  
  # Cache the data set
  saveCache(df, key=key_list)
  
  return(df)
}

df <- get_dataframe('https://osp-rs.stat.gov.lt/rest_xml/', 'S3R629_M3010217')
head(df)

