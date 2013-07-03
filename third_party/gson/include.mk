GSON_VERSION := 2.2.4
GSON := third_party/gson/gson-$(GSON_VERSION).jar
GSON_BASE_URL := http://search.maven.org/remotecontent?filepath=com/google/code/gson/gson/$(GSON_VERSION)

$(GSON): $(GSON).md5
	set dummy "$(GSON_BASE_URL)" "$(GSON)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(GSON)
