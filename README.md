# Characterizing mobility in the COVID-19 mobility restrictions

### **1. Summary**
<p style='text-align: justify;'> This repository contains the experimental part of the Master's thesis entitled 'Characterizing mobility in the COVID-19 mobility restrictions', which has been developed in the context of a Master's Degree in Data Science in UOC (Universitat Oberta de Catalunya), under the supervision of Julian A. Vicens Bennasar. </p>

<p style='text-align: justify;'> The mobility data of this repository is a trips dataset provided by 'Ministerio de Tranportes, Movilidad, y Agenda Urbana' (MITMA) https://www.mitma.gob.es/ministerio/covid-19/evolucion-movilidad-big-data, which it is mainly based on the position of 13 million anonymized mobile phone lines provided by a single mobile operator. The dataset is limited to the Spanish geographical region in the COVID-19 pandemic period (from 29th of February of 2020 to 5th of September of 2021) but in addition, a reference period was also provided for the 15 days before the onset of the pandemic (from 14th to 20th of February of 2020). </p>


<img src="https://external-content.duckduckgo.com/iu/?u=https%3A%2F%2Fak.picdn.net%2Fshutterstock%2Fvideos%2F1016123212%2Fthumb%2F4.jpg&f=1&nofb=1&ipt=064f838d44fb0720e77da3619f67384347e94b3b636acd7e0a6b6ee4caea62af&ipo=images" alt="alt text" title="image Title" height="550"/>

### **2. PostgreSQL Database**
Database derived from the MITMA trips dataset. The included tables are:

 - **mimta_cat_raw**: contains the downloaded trips dataset.
 - **mitma_trips_matrix**: is an origin-destination matrix containing the number of trips between the different regions.
 - **mitma_trips**: this table contains the internal, outgoing, and incoming trips for each region.
 - **mitma_flux**: it is an origin-destination matrix, but instead of containing the number of trips, it includes a mobility index derived from the number of trips that represents the probability of going from one region to another.
 - **mitma_qrp**: contains multiple mobility indexes (q, r and p) for each region derived from the number of trips. 

### **3. Files structure**
Files are organized as follows:

```bash
├───data
│   ├───raw
│   └───processed
│       └───graphs and metrics
│           ├───p_index
│           └───trips
├───img
│   └────bcn
│   └────cat
│   └────tarraco
│   └────lleida
│   └────girona
└───src
│   └───compute_trips.py
│   └───compute_trips_matrix.py
│   └───compute_qrp_indexes.py
│   └───database_utils.py
│   └───mobility_context_and_queries.py
│   └───mobility_plots.py
│
└───01_mobility_analysis_TRIPS.ipynb
└───02_mobility_analysis_INDEXES.ipynb
└───03_graph_catacterization_TRIPS.ipynb
└───04_graph_catacterization_P_index.ipynb

```

- **data**:

    - **raw**: inside there is the 'overlap_mitma_abs.csv' file, which only contains Catalunya's MITMA regions, and a file with the daily number of COVID-19 cases.
    - **processed**:

        - **graphs and metrics**: stores network science metrics and graphs.
            - **p_index**: contains graphs and metrics computed with the p mobility index between MITMA regions.

            - **trips**: contains graphs and metrics computed with the number of trips between MITMA regions.

- **img**: contains all the images produced in the mobility analysis notebooks 

- **src**: scripts and utils

    - **compute_trips.py**: script that computes and stores the mitma_trips table. It includes the incoming, outgoing and internal trips for each MITMA region.

    - **compute_trips_matrix.py**: script that computes and stores the mitma_trips_matrix table. It includes the sum of trips for each pair of MITMA regions.

    - **compute_qrp_indexes.py**: Script that computes and stores mitma_qrp and mitma_flux tables. The first one includes the mobility indexes q, r and p for each MITMA source region and the second the p mobility index fluxes between each pair of regions.

    - **database_utils.py**: functions to create, drop and query to mobility tables.

    - **mobility_context_and_queries.py**: contains lists of locations, phases and motivs to perform the notebook's studies. Also, there are functions to allow easy queries to the PostgreSQL MITMA tables. 

    - **mobility_plot.py**: contains all the functions that allow to visually explore the trips and mobility indexes MITMA data. There are time series  plots, maps and heatmaps.

- notebooks:

    - **01_mobility_analysis_TRIPS.ipynb**: visual exploration of the trips number evolution in Catalunya across the different COVID-19 mobility restrictions. 

    - **02_mobility_analysis_INDEXES.ipynb**: visual exploration of the mobility indexes q, p, r and m in Catalunya across the different COVID-19 mobility restrictions. 

    - **03_graph_catacterization_TRIPS.ipynb**: This notebook finds differences in Catalunya's mobilty across the previously defined COVID-19 phases using network science techniques. In this notebook the focus is in the MITMA mobility index p graphs.   

    - **04_graph_catacterization_P_index.ipynb**: finds differences in Catalunya's mobilty across the previously defined COVID-19 phases using network science techniques. In this notebook the focus is in the numer trips graphs. 

