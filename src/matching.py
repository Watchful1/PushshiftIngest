SEARCH_TERMS = {
	"remindme": ["remindme", "remind me", "remindmerepeat", "cakeday"],
	"updateme": ["updateme", "subscribeme", "subscribeall"],
}

CLIENT_NAMES = list(SEARCH_TERMS.keys())


def match_clients(body):
	"""Return [(client_name, term)] for every client with at least one substring hit.

	Same logic as CommentStreamer's Ingest.check_process_request so both pipelines
	feed the bots identical comments.
	"""
	body_lower = body.lower()
	matches = []
	for client_name, terms in SEARCH_TERMS.items():
		for term in terms:
			if term in body_lower:
				matches.append((client_name, term))
				break
	return matches


def is_valid_author(author):
	return author is not None and author != "[deleted]"
