####################################
# LAFD / TPS Data Cleaning UI Code #
####################################

### Libraries
library(shiny)
library(DT)
library(leaflet)

### User Interface
ui <- fluidPage(
  
  ### Title
  titlePanel("LAFD TPS Matching Utility"),
  
  fluidRow(
    column(4,
           wellPanel(
             # LADOT TPS file input
             fileInput("tps_files",
                       label = 'Upload LADOT TPS logs',
                       multiple = TRUE,
                       accept = ".csv"),
             
             # LAFD .html file input
             fileInput("lafd_files",
                       label = 'Upload LAFD GPS logs',
                       multiple = TRUE,
                       #accept = ".html"),
                       accept = ".csv"),
             
             # Upload summary results text
             #textOutput('result'),
             
             h5(tags$b('Export Run Data')),
             
             # Download button
             downloadButton('downloadData', 'Download')
           )
    ),
    
    column(8,
           # Map Output
           leafletOutput("map", height = 300)
    )
  ),
  
  fluidRow(
    tabsetPanel(
      #id = 'contents',
      tabPanel("Matched Runs", DT::dataTableOutput("matchtable")),
      tabPanel("All Runs", DT::dataTableOutput("alltable"))
      #tabPanel("Unmatched LAFD Runs", DT::dataTableOutput("lafdtable"))
    )
  )
)
  
#   ### Sidebar layout with input 
#   sidebarLayout(
#     
#     # Sidebar panel for inputs 
#     sidebarPanel(
#       
#       # LADOT TPS file input
#       fileInput("tps_files",
#                 label = '1. Upload LADOT TPS logs',
#                 multiple = TRUE,
#                 accept = ".csv"),
#       
#       # LAFD .html file input
#       fileInput("lafd_files",
#                 label = '2. Upload LAFD GPS logs',
#                 multiple = TRUE,
#                 #accept = ".html"),
#                 accept = ".csv"),
#       
#       # Upload summary results text
#       textOutput('result'),
#       
#       # Horizontal line 
#       tags$hr(),
#       
#       h5(tags$b('3. Select table row to view in the map')),
#       
#       # Horizontal line
#       tags$hr(),
#       
#       h5(tags$b('4. Export all clipped data')),
# 
#       # Download button
#       downloadButton('downloadData', 'Download')
#       
#     ),
#     
#   
#     ### Main Output
#     fluidRow(
#       
#       # Left-hand column
#       column(width = 7,
#         
#         # Map Output
#         leafletOutput("map", height = 300),
#         
#         # Results Table
#         #DT::dataTableOutput("contents"),
#         tabsetPanel(
#           #id = 'contents',
#           tabPanel("Matched Runs", DT::dataTableOutput("matchtable")),
#           tabPanel("All Runs", DT::dataTableOutput("alltable"))
#           #tabPanel("Unmatched LAFD Runs", DT::dataTableOutput("lafdtable"))
#         )
#     
#       )
#     )
#   )
# )