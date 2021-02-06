package com.raphtory.test.oag

import java.net.URL

import com.raphtory.examples.oag.PublicationType.PublicationType
import com.raphtory.examples.oag.SourceUrlType.SourceUrlType

case class Author(
                   name: String,
                   org: Option[String]
                 )

case class MAGReference(
                         RId: Long,
                         title: String
                       )

case class DataSource (
                        urlType: SourceUrlType,
                        url: URL
                      )
//"S":[{"Ty":3,"U":"https://web.media.mit.edu/~guyzys/data/ZNP15.pdf"},{"Ty":3,"U":"https://www.weusecoins.com/assets/pdf/library/Decentralizing%20Privacy-%20Using%20Blockchain%20to%20Protect%20Personal%20Data.pdf"}

case class OAGPaper(
                     //identity fields
                     id: Option[Long],
                     doi: Option[String],
                     //  isbn: Option[String],
                     familyId: Option[Long],

                     //naming fields
                     title: Option[String],
                     originalTitle: Option[String], /*
  bookTitle: Option[String],

  //location fields
  pdf: Option[String],  //may be a relative url
  url: Option[List[String]],*/
                     //  sourceType: Option[SourceUrlType],
                     sources: Option[List[DataSource]],
                     //  sources: Option[String],
                     /*
                     docAbstract: Option[String],
                     lang: Option[String],
                     keywords: Option[List[String]],
                                           fos: Option[String],//field of study

                     authors: Option[List[Author]],

                    //publication details
                     docType: Option[String],//TODO change to DocumentType*/
                     publicationType: Option[PublicationType],

                     /*publisher: Option[String],
                     journalId: Option[String],
                     volume: Option[String],
                     issue: Option[String],
                     conferenceSeriesId: Option[String],
                     conferenceInstanceId: Option[String],
                     originalVenue: Option[String],*/
                     year: Option[Int],
                     date: Option[String],
                     /*onlineDate: Option[String],
                     createdDate: Option[String],

                     //citations and references
                     rank: Option[Int], //importance rank
                     // noOfcitation: Option[String],
                     referenceCount: Option[Long],*/
                     citationCount: Option[Int],
                     estimatedCitationCount: Option[Int],
                     references: Option[List[Long]],
                     extendedReferences: Option[List[MAGReference]],
                     isSeed: Option[Boolean],
                     labelDensity: Option[Double]
                     //  citations: Option[List[Long]],
                     //additional non standard-fields for linking to papers not in the id range of the graph
                     //externalReferences: Option[List[String]],
                   )