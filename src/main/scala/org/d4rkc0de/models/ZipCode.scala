package org.d4rkc0de.models

case class ZipCode(
                    RecordNumber: Option[Int] = None,
                    ZipCodeType: Option[String] = None,
                    Zipcode: Option[Int] = None,
                    City: Option[String] = None,
                    State: Option[String] = None,
                    LocationType: Option[String] = None,
                    Lat: Option[Double] = None,
                    Long: Option[Double] = None,
                    Xaxis: Option[Int] = None,
                    Yaxis: Option[Double] = None,
                    Zaxis: Option[Double] = None,
                    WorldRegion: Option[String] = None,
                    Country: Option[String] = None,
                    LocationText: Option[String] = None,
                    Location: Option[String] = None,
                    Decommisioned: Option[Boolean] = None,
                    TaxReturnsFiled: Option[String] = None,
                    EstimatedPopulation: Option[Int] = None,
                    TotalWages: Option[Int] = None,
                    Notes: Option[String] = None,
                  )
