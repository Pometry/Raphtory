package com.raphtory.examples.coho.companiesStream.rawModel.companyProfile

import spray.json.{DefaultJsonProtocol, JsObject, JsString, JsValue, RootJsonFormat}

/**
 * Implicit variables and objects to handle the json objects of the Companies House stream data.
 */

object CompaniesHouseJsonProtocol extends DefaultJsonProtocol {

  implicit val eventFormat = jsonFormat4(Event)
  implicit val confirmationStatementFormat = jsonFormat4(ConfirmationStatement)
  implicit val registeredOfficeAddressFormat = jsonFormat9(RegisteredOfficeAddress)
  implicit val accountingReferenceDateFormat = jsonFormat2(AccountingReferenceDate)
  implicit val lastAccountsFormat = jsonFormat2(LastAccounts)
  implicit val nextAccountsFormat = jsonFormat3(NextAccounts)
  implicit val linksFormat = jsonFormat4(Links)
  implicit val accountPeriodFromFormat = jsonFormat2(AccountPeriodFrom)
  implicit val accountPeriodToFormat = jsonFormat2(AccountPeriodTo)
  implicit val mustFileWithinFormat = jsonFormat1(MustFileWithin)
  implicit val accountingRequirementFormat = jsonFormat2(AccountingRequirement)
  implicit val originatingRegistryFormat = jsonFormat2(OriginatingRegistry)
  implicit val serviceAddressFormat = jsonFormat8(ServiceAddress)
  implicit val annualReturnFormat = jsonFormat4(AnnualReturn)
  implicit val branchCompanyDetailsFormat = jsonFormat3(BranchCompanyDetails)
  implicit val accountsFormat = jsonFormat9(Accounts)
  implicit val foreignCompanyFormat = jsonFormat8(ForeignCompanyDetails)


  def getRawField(field: String)(implicit jsObj: JsObject): Option[JsValue] =
    jsObj.getFields(field).headOption

  def getField(field: String)(implicit jsObj: JsObject): Option[String] =
    getRawField(field) match {
      case Some(s) => Some(s.toString())
      case None => None
    }

  def getInteger(field: String)(implicit jsObj: JsObject): Option[Integer] =
    getRawField(field) match {
      case Some(s) => Some(s.toString().toInt)
      case None => None
    }

  def getBoolean(field: String)(implicit jsObj: JsObject): Option[Boolean] =
    getRawField(field) match {
      case Some(s) => Some(s.toString().toBoolean)
      case None => None
    }

  implicit object PreviousCompanyNamesFormat extends RootJsonFormat[PreviousCompanyNames] {
    override def write(obj: PreviousCompanyNames): JsValue = JsString("TODO")

    override def read(json: JsValue): PreviousCompanyNames = {
      implicit val jsObj = json.asJsObject
      PreviousCompanyNames(
        getField("ceased_on"),
        getField("effective_from"),
        getField("name")
      )
    }
  }

  implicit object DataFormat extends RootJsonFormat[Data] {

    override def write(obj: Data): JsValue = JsString("TODO")

    override def read(json: JsValue): Data = {
      implicit val jsObj = json.asJsObject
      Data(
        getRawField("accounts") match {
          case Some(a) => Some(a.convertTo[Accounts])
          case None => None
        },
        getRawField("annual_return") match {
          case Some(ar) => Some(ar.convertTo[AnnualReturn])
          case None => None
        },
        getRawField("branch_company_details") match {
          case Some(ar) => Some(ar.convertTo[BranchCompanyDetails])
          case None => None
        },
        getBoolean("can_file"),
        getField("company_name"),
        getField("company_number"),
        getField("company_status"),
        getField("company_status_detail"),
        getRawField("confirmation_statement") match {
          case Some(cs) => Some(cs.convertTo[ConfirmationStatement])
          case None => None
        },
        getField("date_of_cessation"),
        getField("date_of_creation"),
        getField("etag"),
        getRawField("foreign_company_details") match {
          case Some(fcd) => Some(fcd.convertTo[ForeignCompanyDetails])
          case None => None
        },
        getBoolean("has_been_liquidated"),
        getBoolean("has_charges"),
        getBoolean("has_insolvency_history"),
        getBoolean("is_community_interest_company"),
        getField("jurisdiction"),
        getField("last_full_members_list_date"),
        getRawField("links") match {
          case Some(l) => Some(l.convertTo[Links])
          case None => None
        },
        getRawField("previous_company_names") match {
          case Some(pcn) => Some(pcn.convertTo[Array[PreviousCompanyNames]])
          case None => None
        },
        getRawField("registered_office_address") match {
          case Some(roa) => Some(roa.convertTo[RegisteredOfficeAddress])
          case None => None
        },
        getBoolean("registered_office_is_in_dispute"),
        getRawField("service_address") match {
          case Some(sa) => Some(sa.convertTo[ServiceAddress])
          case None => None
        },
        getField("sic_codes"),
        getField("_type"),
        getBoolean("undeliverable_registered_office_address")
      )
    }
  }

  implicit object CompanyFormat extends RootJsonFormat[Company] {

    override def write(obj: Company) = JsString("TODO")

    override def read(json: JsValue): Company = {
      implicit val jsObj = json.asJsObject

      Company(
        getField("resource_kind"),
        getField("resource_uri"),
        getField("resource_id"),
        getRawField("data") match {
          case Some(d) => Some(d.convertTo[Data])
          case None => None
        },
        getRawField("event") match {
          case Some(e) => Some(e.convertTo[Event])
          case None => None
        }
      )
    }
  }


  implicit object AccountsFormat extends RootJsonFormat[Accounts] {

    override def write(obj: Accounts): JsValue = JsString("TODO")

    override def read(json: JsValue): Accounts = {
      implicit val jsObj = json.asJsObject
      Accounts(
        getRawField("account_period_from") match {
          case Some(apf) => Some(apf.convertTo[AccountPeriodFrom])
          case None => None
        },
        getRawField("account_period_to") match {
          case Some(apt) => Some(apt.convertTo[AccountPeriodTo])
          case None => None
        },
        getRawField("accounting_reference_date") match {
          case Some(ard) => Some(ard.convertTo[AccountingReferenceDate])
          case None => None
        },
        getRawField("last_accounts") match {
          case Some(la) => Some(la.convertTo[LastAccounts])
          case None => None
        },
        getRawField("must_file_within") match {
          case Some(mfw) => Some(mfw.convertTo[MustFileWithin])
          case None => None
        },
        getRawField("next_accounts") match {
          case Some(na) => Some(na.convertTo[NextAccounts])
          case None => None
        },
        getField("next_due"),
        getField("next_made_up_to"),
        getBoolean("overdue")
      )
    }
  }

  implicit object ForeignCompanyDetailsFormat extends RootJsonFormat[ForeignCompanyDetails] {
    override def write(obj: ForeignCompanyDetails): JsValue = JsString("TODO")

    override def read(json: JsValue): ForeignCompanyDetails = {
      implicit val jsObj = json.asJsObject
      ForeignCompanyDetails(
        getRawField("accounts") match {
          case Some(a) => Some(a.convertTo[Accounts])
          case None => None
        },
        getRawField("accounting_requirement") match {
          case Some(ar) => Some(ar.convertTo[AccountingRequirement])
          case None => None
        },
        getField("business_activity"),
        getField("company_type"),
        getField("governed_by"),
        getBoolean("is_a_credit_finance_institution"),
        getRawField("originating_registry") match {
          case Some(or) => Some(or.convertTo[OriginatingRegistry])
          case None => None
        },
        getField("registration_number")
      )
    }
  }


}
