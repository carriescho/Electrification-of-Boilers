## Electrification-of-Boilers

This repository is for the code and data used in our project evaluating the electrification of industrial boilers in the United States.

# Project objectives: 
1) Develop an up-to-date inventory of industrial boilers used in US manufacturing industries
2) Characterize industrial boilers by boiler capacities, fuel types, 3-digit NAICS subsectors, and counties in the US
3) Calculate the electricity requirement for electric boilers to replace steam demand from conventional fossil fuel boilers and the resulting fuel use and GHG emissions from electrification based on regional electric grid mixes for the current grid and future scenarios

# Overview of directories
- [/GHGRP/](https://github.com/carriescho/Electrification-of-Boilers/tree/master/GHGRP) contains the code for assembling industrial boiler data from the [EPA's GHGRP](https://www.epa.gov/ghgreporting/find-and-use-ghgrp-data) database.
- [/MACT/](https://github.com/carriescho/Electrification-of-Boilers/tree/master/MACT) contains the code for assembling industrial boiler data from the [EPA's MACT](https://www.epa.gov/stationary-sources-air-pollution/industrial-commercial-and-institutional-boilers-and-process-heaters) database.
- [/NEI/](https://github.com/carriescho/Electrification-of-Boilers/tree/master/NEI) contains the code for assembling industrial boiler data from the [EPA's NEI](https://www.epa.gov/air-emissions-inventories/2017-national-emissions-inventory-nei-data) database.
- [/boiler_inventory/](https://github.com/carriescho/Electrification-of-Boilers/tree/master/boiler_inventory) contains the code for combining and cross-checking data of boiler units reported in the GHGRP & MACT and NEI and for expanding the boiler inventory to account for non-reported boilers based on county-level boiler fuel use estimates.
- [/electrification_potential/](https://github.com/carriescho/Electrification-of-Boilers/tree/master/electrification_potential) contains the code for calculating the electrification potential (electricity required for electric boilers to meet steam demand), the resulting fuel use from electricity, and net changes in GHG emissions from boiler electrification.
- [/updated_MACT_EPA/](https://github.com/carriescho/Electrification-of-Boilers/tree/master/updated_MACT_EPA) contains the code for combining and cross-checking data of boiler units reported in the GHGRP and MACT.
- The file, total_boiler_inventory.csv, is our up-to-date industrial boiler dataset for the US.
