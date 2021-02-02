package com.raphtory.examples.oag

object PublicationType extends Enumeration {
  type PublicationType = Value

  //  0:Unknown, 1:Journal article, 2:Patent, 3:Conference paper, 4:Book chapter, 5:Book, 6:Book reference entry, 7:Dataset, 8:Repository
  val Unknown: Value = Value("Unknown")
  val JournalArticle: Value = Value("JournalArticle")
  val Patent: Value = Value("Patent")
  val ConferencePaper: Value = Value("ConferencePaper")
  val BookChapter: Value = Value("BookChapter")
  val Book: Value = Value("Book")
  val BookReferenceEntry: Value = Value("BookReferenceEntry")
  val Dataset: Value = Value("Dataset")
  val Repository: Value = Value("Repository")
}
