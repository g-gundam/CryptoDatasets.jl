using Dates, NanoDates

const Time0 = NanoDate(DateTime(1970,1,1)) # UNIX 

millis2nanodate(millis::Millisecond) = Time0 + millis

millis_today     = Millisecond( now() - Time0)
millis_lastyear = Millisecond( (now() - Year(1)) - Time0)
@info millis_today, millis_lastyear

today     = millis2nanodate(millis_today)
lastyear = millis2nanodate(millis_lastyear)
@info today, lastyear
@info typeof(today), typeof(lastyear)
