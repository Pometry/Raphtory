package com.raphtory.examples.oag

case class OpenAcademic(
/*"title": "1 Blockchain's roles in meeting key supply chain management objectives",
"authors.name": ["N. Kshetri"],
"year": 2018,
"Corpus_ID": "46783030",
"doi": "10.1016/j.ijinfomgt.2017.12.005",
"fos": "Business, Computer Science",
"publisher":"Int. J. Inf. Manag.",
"pdf": "http://libres.uncg.edu/ir/uncg/f/N_Kshetri_Blockchains_Roles_2018.pdf",
"bibtex_citation": "@article{Kshetri20181BR,
        title={1 Blockchain's roles in meeting key supply chain management objectives},
author={N. Kshetri},
journal={Int. J. Inf. Manag.},
year={2018},
volume={39},
pages={80-89}
}",
    "references":[{
*/


  //naming fields
  title: Option[String],
  doi: Option[String],
  paperId: Option[String],
  year: Option[Int],
  references: Option[List[OpenAcademic]],
  citations: Option[List[OpenAcademic]],
  isSeed: Option[Boolean],
  labelDensity: Option[Double]
)
