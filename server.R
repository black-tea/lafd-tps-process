########################################
# LAFD / TPS Data Cleaning Server Code #
########################################

### Libraries
require(rvest)
library(magrittr)
library(stringr)
library(sf)
library(leaflet)
library(dplyr)
library(DT) # Install using dev_tools::github
library(fuzzyjoin)
library(IRanges)
library(lubridate)
library(units)

options(tibble.print_max = Inf)

### Load and Prep Data
# Venice Boundary
tps_extent <- st_read('data/eval_extent/tsp-extent_line.shp')
# Loop Detector Location 
detectors <- read.csv('data/signal/detectors.csv', header = TRUE, sep = ',', stringsAsFactors = FALSE)
# Loop Tag ID Pairs
lafd_veh_id <- c('E62', 'RA62','E83','RA83')
tps_tag_id <- c(6598, 5614,5176,5880)
tag_tbl <- data.frame(tps_tag_id, lafd_veh_id)

### Support Functions
# Function to buffer in Nad83 and Return in WGS84
geom_buff <- function(boundary, ft) {
  geom_nad83 <- st_transform(boundary, 2229) # Convert to NAD83
  geom_nad83 <- st_buffer(geom_nad83, ft) # Buffer
  geom_wgs84 <- st_transform(geom_nad83, 4326) # Convert back to wgs84
  return(geom_wgs84)
}

# Function to process LAFD raw gps file into a spatial df
lafd_process <- function(datapath, fname) {
  
  # Extract veh_id
  #veh_id <- strsplit(fname, "_")[[1]][1]
  
  # Read in LAFD Data
  lafd_data <- read.csv(datapath,
                        header = TRUE,
                        sep = ',',
                        stringsAsFactors = FALSE)
  
  lafd_data <- lafd_data %>%
    mutate(incident_id = as.numeric(INCIDENT_NBR),
           #veh_id = veh_id,
           veh_id = UNIT_NAME,
           status = UNIT_STATUS,
           lat = as.numeric(LATITUDE),
           lon = as.numeric(LONGITUDE),
           timestmp = as.POSIXct(UTC_TIME,
                                 format = "%d-%b-%y %I.%M.%S.000000 %p",
                                 tz = "UTC")) %>%
    dplyr::select(incident_id,
                  veh_id,
                  status,
                  lat,
                  lon,
                  timestmp) %>%
    # Filter for only those gps points enroute, excluding ONS = onsite
    filter(status == 'ENR')
  
  # Convert to Los Angeles tz
  attributes(lafd_data$timestmp)$tzone <- "America/Los_Angeles"
  
  lafd_sf <- st_as_sf(lafd_data,
                      coords=c("lon","lat"),
                      crs=4326,
                      remove=FALSE)
  
  return(lafd_sf)
}

# Function to process TPS csv file into a spatial df
tps_process <- function(datapath) {
  
  # Read in Loop Data
  loop_data <- read.csv(datapath,
                        header = TRUE,
                        sep = ',',
                        stringsAsFactors = FALSE
  )
  
  loop_data <- loop_data %>%
    # Convert timestamp to date/time value
    mutate(#RECID = as.numeric(RECID),
           # lubridate package to supply vector of possible datetime character formats
           TIMESTMP = parse_date_time(TIMESTMP,
                                      orders = c("%m/%d/%Y %H:%M:%S",
                                                 "%Y-%m-%d %H:%M:%S"),
                                      tz="America/Los_Angeles"),
           CON_ID = as.numeric(CON_ID),
           DET_ID = as.numeric(DET_ID),
           TAG_ID = as.numeric(TAG_ID),
           ERRORCODE = as.numeric(ERRORCODE),
           ERRORMSG = as.character(ERRORMSG),
           LATENESS = as.numeric(LATENESS),
           SYSID = as.numeric(SYSID),
           VEH_NUM = as.character(VEH_NUM),
           EVFLAG = as.numeric(EVFLAG)) %>%
    # Filter out erroneous dates created by null date values
    filter(TIMESTMP > "2017-01-01")
    
  loop_data <- loop_data %>%
    # Join to detector location information
    left_join(detectors, by='DET_ID') %>%
    # Only want "ISSUED" error codes (68111, 68112); also remove lat/lon NA values
    mutate(
      lat = as.numeric(lat),
      lon = as.numeric(lon)
    ) %>%
    dplyr::rename('tps_tag_id' = 'TAG_ID') %>%
    filter(ERRORCODE %in% c(68111,68112)
           ,!is.na(lat)
           ,!is.na(lon))
  
  # Convert df to sf object
  loop_sf <- st_as_sf(loop_data,
                      coords=c("lon","lat"),
                      crs=4326,
                      remove=FALSE)
  
  return(loop_sf)
}

# Function to filter points to project boundary
sf_points_filter <- function(spatial_df) {
  
  # Filter points within project boundary
  sf_df_filtered <- spatial_df[tps_extent_buff,]
  
  # Only return if it has at least two points in project boundary
  if(nrow(sf_df_filtered) > 1){
    return(sf_df_filtered)
  } else {
    return(NULL)
  }
  
}

# Function to create Icons for map
createIcon <- function(color) {
  
  custom_icon <- awesomeIcons(
    icon = 'circle-o',
    iconColor = '#ffffff',
    library = 'fa',
    markerColor = color
  )
  
  return(custom_icon)
  
}
 
### Load and Prep Data
tps_extent_buff <- geom_buff(tps_extent, 100)

### Server Code
server <- function(input, output) {
  
  # Reactive expression to process LADOT TPS logs
  tps_log <- reactive({
    
    if(!is.null(input$tps_files)) {
      
      # Process each TPS Log
      tps_log <- lapply(input$tps_files$datapath, tps_process)
      
      # Combine list of dfs into one sf df
      tps_log <- do.call(rbind, tps_log)
      
      tps_log <- tps_log %>%
        group_by(tps_tag_id) %>%
        arrange(TIMESTMP, .by_group = TRUE) %>%
        # Calculate lag time between each timestamp and one before it
        mutate(TIMESTMP_LAG = ifelse(!is.na(lag(TIMESTMP)),
                                     TIMESTMP - lag(TIMESTMP),
                                     NA)) %>%
        # Create break point where there is 3 min gap btw last event, assign Run ID
        mutate(run_flag = ifelse(is.na(TIMESTMP_LAG) | TIMESTMP_LAG > 360,
                                 1,
                                 0)) %>%
        ungroup() %>%
        # Assign Run ID
        mutate(run_id = cumsum(run_flag)) %>%
        # Reorder the group IDs to be based on timestamp (not by TPS tag ID)
        group_by(run_id) %>%
        mutate(mean_time = mean(TIMESTMP)) %>%
        ungroup() %>%
        arrange(mean_time) %>%
        # Create new run_id based on (1) mean_time and current run_id
        mutate(run_id = group_indices_(., .dots=c("mean_time", "run_id"))) %>%
        select(
          -run_flag,
          -mean_time,
          -TIMESTMP_LAG
        )

      return(tps_log)
    }
  })
  
  # Reactive expression to convert tps_logs() into "runs"
  tps_runs <- reactive({
    
    # Group by run, get start/end
    tps_runs <- tps_log() %>%
      group_by(run_id, tps_tag_id) %>%
      summarise(
        start = min(TIMESTMP),
        end = max(TIMESTMP)
      ) %>%
      st_set_geometry(NULL)
    
    return(tps_runs)
  })
  
  # Reactive expression to process files to list of sf dfs
  lafd_points <- reactive({
    
    if(!is.null(input$lafd_files)) {
      
      # Set simplify = FALSE to return list instead of matrix
      lafd_points <- mapply(lafd_process
                            ,input$lafd_files$datapath
                            ,input$lafd_files$name
                            ,SIMPLIFY = FALSE
      )
      
      # Combine list of dfs into one sf df
      lafd_points <- do.call(rbind, lafd_points)
      
      # Filter by project limits
      lafd_points <- lafd_points[tps_extent_buff,]
      
      lafd_log <- lafd_points %>%
        dplyr::group_by(incident_id, veh_id) %>%
        # add .by_group to preserve previous grouping
        arrange(timestmp, .by_group = TRUE) %>%
        # Calculate lag time between each timestamp and one before it
        mutate(timestmp_lag = ifelse(!is.na(lag(timestmp)),
                                     timestmp - lag(timestmp),
                                     0)) %>%
        # Create break point at each new incident_id number 
        # or where there is 3 min gap btw last event
        mutate(run_flag = ifelse(timestmp_lag == 0 | timestmp_lag > 360,
                                 1,
                                 0)) %>%
        ungroup() %>%
        # Assign Run ID
        mutate(run_id = cumsum(run_flag)) %>%
        group_by(run_id) %>%
        # Filter for > 2 points
        filter(n() >= 2) %>%
        ungroup() %>%
        mutate(run_id = cumsum(run_flag)) %>%
        mutate(run_id = as.integer(run_id))
      
      # Return final list of sf dfs
      return(lafd_log)
      
    } else {
      return(NULL)
    }
    
  })
  
  # Reactive expression converting LAFD points to segments
  lafd_paths <- reactive({
    
    if(!is.null(lafd_points())){
      
      # Merge points into linestring
      lafd_paths <- lafd_points() %>%
        group_by(run_id, incident_id, veh_id) %>%
        summarize(start = min(timestmp),
                  end = max(timestmp),
                  Time.Sec = difftime(max(timestmp),min(timestmp),units='secs'),
                  do_union=FALSE) %>%
        st_cast("LINESTRING") %>%
        # Calculate length and speed on corridor
        mutate(
          Miles = round((st_length(st_transform(geometry, 2229))/5280),2),
          MPH = round((Miles/(Time.Sec/3600)),2)
        ) %>%
        select(
          run_id,
          incident_id,
          veh_id,
          start,
          end,
          Miles,
          MPH
        )
      
      return(lafd_paths)
      
      
    } else {
      return(NULL)
    }
  })
  
  
  ### All Reactive expressions involving the table joins
  # First table join combining LADOT & LAFD Data
  join_tbl <- reactive({

    # Only process if both LAFD & LADOT data is not null
    if((!is.null(tps_runs()))&(!is.null(lafd_points()))) {
      
      # Format time, remove geometry column, add tps_tag_id
      lafd_paths_tbl <- lafd_paths() %>%
        st_set_geometry(NULL) %>%
        left_join(tag_tbl, by=c('veh_id' = 'lafd_veh_id')) %>%
        # Genome join requires numeric, so convert posixct -> numeric
        mutate(start = as.numeric(start),
               end = as.numeric(end))
      print(lafd_paths_tbl)
      print(tag_tbl)
      
      tps_runs_tbl <- tps_runs() %>%
        # Genome join requires numeric, so convert posixct -> numeric
        mutate(start = as.numeric(start),
               end = as.numeric(end))

      # Genome join from 'fuzzyjoin' package to identify overlap between time intervals with additional ID field
      join_tbl <- tps_runs_tbl %>%
        genome_full_join(lafd_paths_tbl, by=c("tps_tag_id","start",'end')) %>%
        # Convert numeric back to dates (need to add origin variable)
        mutate(
          start.x = as.POSIXct(start.x, origin="1970-01-01", tz = "America/Los_Angeles"),
          end.x = as.POSIXct(end.x, origin="1970-01-01", tz = "America/Los_Angeles"),
          start.y = as.POSIXct(start.y, origin="1970-01-01", tz = "America/Los_Angeles"),
          end.y = as.POSIXct(end.y, origin="1970-01-01", tz = "America/Los_Angeles")
        ) %>%
        mutate(
          time = ifelse(!is.na(start.x),
                        start.x,
                        start.y)
        ) %>%
        arrange(time) %>%
        # Convert to character for display
        mutate(
          start.x = as.character(start.x),
          end.x = as.character(end.x),
          start.y = as.character(start.y),
          end.y = as.character(end.y)
        ) %>%
        ungroup() %>%
        # Assign new Run ID value (that combines LAFD & TPS)
        mutate(RunID = 1:length(time)) %>%
        select(
          RunID,
          run_id.x,
          tps_tag_id.x,
          start.x,
          end.x,
          run_id.y,
          incident_id,
          veh_id,
          start.y,
          end.y,
          Miles,
          MPH
        ) %>%
        dplyr::rename('TPS.RunID' = 'run_id.x') %>%
        dplyr::rename('TPS.TagID' = 'tps_tag_id.x') %>%
        dplyr::rename('TPS.Start' = 'start.x') %>%
        dplyr::rename('TPS.End' = 'end.x') %>%
        dplyr::rename('LAFD.RunID' = 'run_id.y') %>%
        dplyr::rename('Incident' = 'incident_id') %>%
        dplyr::rename('Veh.ID' = 'veh_id') %>%
        dplyr::rename('LAFD.Start' = 'start.y') %>%
        dplyr::rename('LAFD.End' = 'end.y')
      
      # Return joined table
      return(join_tbl)

      } else {
      return(NULL)
    }

  })
  
  # Post-Join match table
  match_tbl <- reactive({

    match_tbl <- join_tbl() %>%
      filter(!is.na(TPS.RunID)) %>%
      filter(!is.na(Incident)) 
    print(match_tbl)
    # Return formatted table
    return(match_tbl)

  })

  
  ### UI Text Output
  # Text Summarizing Clipping Results
  output$result <- renderPrint({
    
    # Input variables
    upload_ct <- nrow(input$lafd_files)
    venice_ct <- nrow(lafd_paths()) 
    
    # Output text
    if (!is.null(input$lafd_files)) {
      cat('Of the total '
          ,upload_ct
          ,' LAFD trips uploaded, '
          ,venice_ct
          ,' trips have segments within the project area.')
    }
    
  })
  
  ### UI Table Output
  # Matched Rows
  output$matchtable <- DT::renderDataTable({

    # If not null, process files and output
    if((!is.null(input$lafd_files))&(!is.null(input$tps_files))) {
      #print('match table!!!')
      #print(match_tbl())
      #return(match_tbl())
      #Return formatted table
      return(match_tbl() %>%
               select(
                 -TPS.RunID,
                 -LAFD.RunID
               )
             )
      
    } else {

      return(NULL)
    }
    
  },
  # Restrict it to only one row selected at a time
  selection = 'single',
  rownames = FALSE
  )
  
  # Unmatched LAFD Table
  output$alltable <- DT::renderDataTable({
    
    # Process once both inputs have been uploaded
    if((!is.null(input$lafd_files))&(!is.null(input$tps_files))) {
      
      return(join_tbl() %>%
               select(
                 -TPS.RunID,
                 -LAFD.RunID
               )
             )
      
    } else {
      return(NULL)
    }
    
  },
  # Restrict it to only one row selected at a time
  selection = 'single',
  rownames = FALSE
  )
  
  ### Reactive Expressions to get data from selected rows
  # List of all points/paths from selected rows of matched data
  match_pts_s <- reactive({
    if(!is.null(input$matchtable_rows_selected)){
      
      # Get selected rows
      row_num <- input$matchtable_rows_selected
      
      # TPS Points
      tps_run_num <- as.integer(match_tbl()[row_num, 2])
      tps_points_s <- tps_log() %>%
        filter(run_id == tps_run_num)
      
      # LAFD Points
      lafd_incident <- as.numeric(match_tbl()[row_num, 7])
      lafd_veh <- as.character(match_tbl()[row_num,8])
      lafd_run <- as.numeric(match_tbl()[row_num,6])
      lafd_points_s <- lafd_points() %>%
        filter(incident_id == lafd_incident & veh_id == lafd_veh & run_id == lafd_run)
      
      # LAFD Paths
      lafd_paths_s <- lafd_paths() %>%
        filter(incident_id == lafd_incident & veh_id == lafd_veh & run_id == lafd_run)
      
      # Create list of data
      match_pts_s <- list(
        tps_points_s = tps_points_s,
        lafd_points_s = lafd_points_s,
        lafd_paths_s = lafd_paths_s
      )
      
      return(match_pts_s)
      
    } else {
      return(NULL)
    }
  })
  
  # Grab points from selected row of 'all' table
  all_pts_s <- reactive({
    if(!is.null(input$alltable_rows_selected)){
      
      # Get selected rows
      row_num <- input$alltable_rows_selected
      
      # TPS Points
      if(!is.null(join_tbl()[row_num, 2])){
        tps_run_num <- as.integer(join_tbl()[row_num, 2])
        tps_points_s <- tps_log() %>%
          filter(run_id == tps_run_num)
      } else {
        tps_points_s <- NULL
      }
      
      # LAFD Points
      if(!is.null(join_tbl()[row_num, 7])){
        lafd_incident <- as.numeric(join_tbl()[row_num, 7])
        lafd_veh <- as.character(join_tbl()[row_num,8])
        lafd_run <- as.numeric(join_tbl()[row_num,6])
        lafd_points_s <- lafd_points() %>%
          filter(incident_id == lafd_incident & veh_id == lafd_veh & run_id == lafd_run)
      } else {
        lafd_points_s <- NULL
      }
      
      # LAFD Paths
      if(!is.null(join_tbl()[row_num, 7])){
        lafd_paths_s <- lafd_paths() %>%
          filter(incident_id == lafd_incident & veh_id == lafd_veh & run_id == lafd_run)
      } else {
        lafd_paths_s <- NULL
      }
      
      # Create list of data
      all_pts_s <- list(
        tps_points_s = tps_points_s,
        lafd_points_s = lafd_points_s,
        lafd_paths_s = lafd_paths_s
      )
      
      #print(all_pts_s)
      
      return(all_pts_s)

    }
  })
  
  ### Map Object
  # Render the Leaflet Map (based on reactive map object)
  output$map <- renderLeaflet({
    
    # Map object
    map <- leaflet() %>%
      addProviderTiles(providers$Stamen.TonerLite)
    
    # Return final map
    map
    
  })
  
  ### Map Observer Objects
  # Observer focused on Matched Runs
  observe({

    if(!is.null(input$matchtable_rows_selected)) {

      #lafd_points_s <- lafd_points_s()
      match_pts_s <- match_pts_s()

      # Erase markers/shapes and add new lafd ones
      leafletProxy("map") %>%
        clearShapes() %>%
        clearMarkers() %>%
        addPolylines(
          color = '#fc0307',
          weight = 3,
          opacity = 1,
          data = match_pts_s$lafd_paths_s
        ) %>%
        addAwesomeMarkers(
          data = match_pts_s$lafd_points_s,
          icon = createIcon('red'),
          label = as.character(match_pts_s$lafd_points_s$timestmp)
        ) %>%
        addAwesomeMarkers(
          data = match_pts_s$tps_points_s,
          icon = createIcon('blue'),
          label = ~as.character(match_pts_s$tps_points_s$TIMESTMP)
        ) %>%
        # Update the map zoom bounds
        fitBounds(lng1 = max(match_pts_s$lafd_points_s$lon),
                  lat1 = max(match_pts_s$lafd_points_s$lat),
                  lng2 = min(match_pts_s$lafd_points_s$lon),
                  lat2 = min(match_pts_s$lafd_points_s$lat)
        )
    }
  })
  
  # Observer focused on All Runs
  observe({
    
    if(!is.null(input$alltable_rows_selected)) {
      
      all_pts_s <- all_pts_s()
      
      # Erase markers/shapes and add new lafd ones
      leafletProxy("map") %>%
        clearShapes() %>%
        clearMarkers() 

      # Add LADOT points (if not null)
      if(nrow(all_pts_s$tps_points_s) > 0){
        leafletProxy("map") %>%
          addAwesomeMarkers(
            data = all_pts_s$tps_points_s
            ,icon = createIcon('blue')
            ,label = ~as.character(all_pts_s$tps_points_s$TIMESTMP)
          )
      }
      
      # Add LAFD points (if not null)
      if(nrow(all_pts_s$lafd_points_s) > 0){
        leafletProxy("map") %>%
          addAwesomeMarkers(
            data = all_pts_s$lafd_points_s
            ,icon = createIcon('red')
            ,label = as.character(all_pts_s$lafd_points_s$timestmp)
          ) %>%
          # Add LAFD run polyline
          addPolylines(
            color = '#fc0307',
            weight = 3,
            opacity = 1,
            data = all_pts_s$lafd_paths_s
          ) %>%
          # Update the map zoom bounds
          fitBounds(lng1 = max(all_pts_s$lafd_points_s$lon),
                    lat1 = max(all_pts_s$lafd_points_s$lat),
                    lng2 = min(all_pts_s$lafd_points_s$lon),
                    lat2 = min(all_pts_s$lafd_points_s$lat)
          )
          
      } else {
        
        # Update map zoom bounds to be based on TPS points
        leafletProxy("map") %>%
          fitBounds(lng1 = max(all_pts_s$tps_points_s$lon),
                    lat1 = max(all_pts_s$tps_points_s$lat),
                    lng2 = min(all_pts_s$tps_points_s$lon),
                    lat2 = min(all_pts_s$tps_points_s$lat)
          )
        
      }
    }
  })
  
  #### Download data
  output$downloadData <- downloadHandler(
    filename = function() {
      paste("data-", Sys.Date(), ".csv", sep="")
    },
    content = function(file) {
      write.csv(join_tbl(), file)
    }
  )
  
}