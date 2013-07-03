JODATIME_VERSION := 2.2
JODATIME := third_party/jodatime/joda-time-$(JODATIME_VERSION).jar
JODATIME_BASE_URL := http://repo1.maven.org/maven2/joda-time/joda-time/$(JODATIME_VERSION)

$(JODATIME): $(JODATIME).md5
	set dummy "$(JODATIME_BASE_URL)" "$(JODATIME)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(JODATIME)
