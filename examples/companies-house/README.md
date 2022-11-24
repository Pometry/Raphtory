# Companies House Example

## Overview
This is the code used to analyse the Companies House data in Raphtory, written up in two blog posts: [A Series Of Unfortunate Directors](https://www.raphtory.com/A-Series-of-Unfortunate-Directors/) and [The Chocolate NePPEtism Factory](https://www.raphtory.com/NHS-contracts/)

## Data

The data was pulled from the [Companies House Public API](https://developer-specs.company-information.service.gov.uk/) and it is in JSON format. 
We have provided the JSON models for the [Company Profile](https://developer-specs.company-information.service.gov.uk/companies-house-public-data-api/reference/company-profile), 
[Officer Appointment List](https://developer-specs.company-information.service.gov.uk/companies-house-public-data-api/reference/officer-appointments), 
[List Officers of a Company](https://developer-specs.company-information.service.gov.uk/companies-house-public-data-api/reference/officers/list) 
and [List Persons with Significant Control](https://developer-specs.company-information.service.gov.uk/companies-house-public-data-api/reference/persons-with-significant-control/list) resources.

## Code

### Scala
All the Scala code used to analyse Companies House Data in Raphtory can be found in the `scala` directory.

### Python
All the Python code used to analyse the Raphtory output can be found in the `python` directory.
