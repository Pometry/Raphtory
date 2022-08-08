# A Series of Unfortunate Directors

## Overview
This is the code used to analyse the Companies House data in Raphtory and gave us the results in this [blog post](https://www.raphtory.com/A-Series-of-Unfortunate-Directors/).
In this project, we used a simple Edge List algorithm to find the number of companies a director was appointed to and resigned on over time. You can read more about our results here: https://www.raphtory.com/A-Series-of-Unfortunate-Directors/

## Data

The data was pulled from the [Companies House Public API](https://developer-specs.company-information.service.gov.uk/) and it is in JSON format. We have provided the JSON models for the [Company Profile](https://developer-specs.company-information.service.gov.uk/companies-house-public-data-api/reference/company-profile), [Officer Appointment List](https://developer-specs.company-information.service.gov.uk/companies-house-public-data-api/reference/officer-appointments), [List Officers of a Company](https://developer-specs.company-information.service.gov.uk/companies-house-public-data-api/reference/officers/list) and [List Persons with Significant Control](https://developer-specs.company-information.service.gov.uk/companies-house-public-data-api/reference/persons-with-significant-control/list) resources.

## Code

### Scala
`src/main/scala/` contains all the scala code used in Raphtory.

- `CompaniesHouseTest` runs the code used to output results in the blog post.
- The directory `graphbuilders` contains several graph builders used to map the data, `OfficerToCompanyGraphBuilder` was used to build the graph in this [blog post](https://www.raphtory.com/A-Series-of-Unfortunate-Directors/).
- The raw JSON models used to parse JSON in scala can be found in the directory `rawModel`.

### Python
The python notebook for analysis used in the [blog post](https://www.raphtory.com/A-Series-of-Unfortunate-Directors/) is in the `python` directory called `CompaniesHouseNotebook.ipynb`.
