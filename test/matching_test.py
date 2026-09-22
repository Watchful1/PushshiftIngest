import matching


def test_each_remindme_term_matches_remindme():
	for term in ["remindme", "remind me", "remindmerepeat", "cakeday"]:
		# "remindmerepeat" always also contains "remindme", which is listed first,
		# so it is the term reported back (see test_first_matching_term_reported).
		expected_term = "remindme" if term == "remindmerepeat" else term
		assert matching.match_clients(f"hello {term} 1 day") == [("remindme", expected_term)]


def test_each_updateme_term_matches_updateme():
	for term in ["updateme", "subscribeme", "subscribeall"]:
		assert matching.match_clients(f"{term}!") == [("updateme", term)]


def test_case_insensitive():
	assert matching.match_clients("RemindMe! 2 weeks") == [("remindme", "remindme")]


def test_first_matching_term_reported():
	# "remindmerepeat" contains "remindme", and "remindme" is listed first
	assert matching.match_clients("remindmerepeat! 1 day") == [("remindme", "remindme")]


def test_both_clients_match():
	result = matching.match_clients("RemindMe! 1 day and UpdateMe!")
	assert result == [("remindme", "remindme"), ("updateme", "updateme")]


def test_no_match():
	assert matching.match_clients("just a normal comment") == []


def test_valid_author():
	assert matching.is_valid_author("Watchful1")
	assert not matching.is_valid_author(None)
	assert not matching.is_valid_author("[deleted]")


def test_classify_remindme_command():
	assert matching.classify("remindme", "RemindMe! 1 day") == ("remindme", "command")
	assert matching.classify("remindme", "!RemindMe 1 day") == ("remindme", "command")


def test_classify_remindmerepeat_command():
	assert matching.classify("remindme", "RemindMeRepeat! 1 week") == ("remindmerepeat", "command")


def test_classify_cakeday():
	assert matching.classify("remindme", "Cakeday!") == ("cakeday", "command")
	assert matching.classify("remindme", "happy cakeday to you") == ("cakeday", "prose")


def test_classify_remind_me_with_space():
	assert matching.classify("remindme", "Remind me! 5 months") == ("remind me", "command")
	assert matching.classify("remindme", "they remind me of home") == ("remind me", "prose")
	assert matching.classify("remindme", "blah\nremind me! 2 days") == ("remind me", "command")


def test_classify_bare_mention_vs_command():
	assert matching.classify("remindme", "u/RemindMeBot 2 days") == ("remindme", "mention")
	assert matching.classify("remindme", "hey u/remindmebot remindme! 2 days") == ("remindme", "command")


def test_classify_updateme_command_precedence():
	assert matching.classify("updateme", "UpdateMe!") == ("updateme", "command")
	assert matching.classify("updateme", "subscribeme and updateme") == ("subscribeme", "command")
	assert matching.classify("updateme", "SubscribeAll!") == ("subscribeall", "command")


def test_classify_no_term_is_unknown_prose():
	assert matching.classify("remindme", "just a normal comment") == ("unknown", "prose")
