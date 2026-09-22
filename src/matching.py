import re

SEARCH_TERMS = {
	"remindme": ["remindme", "remind me", "remindmerepeat", "cakeday"],
	"updateme": ["updateme", "subscribeme", "subscribeall"],
}

CLIENT_NAMES = list(SEARCH_TERMS.keys())

KIND_COMMAND = "command"
KIND_MENTION = "mention"
KIND_PROSE = "prose"
KINDS = (KIND_COMMAND, KIND_MENTION, KIND_PROSE)

# RemindMeBot's own rules, copied from RemindMeBot/src/comments.py so the classification
# matches what the bot will actually do with a row.
MENTION_PATTERN = re.compile(r"/?u/remindmebot(?:\s+(repeat|cakeday))?\b")


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


def _trigger_in_text(body, trigger):
	return f"{trigger}!" in body or f"!{trigger}" in body


def _trigger_start_of_line(body, trigger):
	for line in body.splitlines():
		if line.startswith(f"{trigger}!") or line.startswith(f"!{trigger}"):
			return True
	return False


def classify(client, body):
	"""Return (term, kind) for a body already known to match this client.

	kind is command when the bot would act on the row, mention for a bare u/RemindMeBot
	mention with no command (handled by the bot's inbox path, so the ingest row is
	skipped), and prose otherwise. term is the trigger the bot would parse under its
	own precedence for a command, or the first substring hit for anything else.
	"""
	lower = body.lower().strip()
	if client == "remindme":
		if _trigger_in_text(lower, "remindmerepeat"):
			return "remindmerepeat", KIND_COMMAND
		if _trigger_in_text(lower, "remindme"):
			return "remindme", KIND_COMMAND
		if _trigger_start_of_line(lower, "cakeday"):
			return "cakeday", KIND_COMMAND
		if _trigger_start_of_line(lower, "remind me"):
			return "remind me", KIND_COMMAND
		if MENTION_PATTERN.search(lower) is not None:
			return "remindme", KIND_MENTION
	else:
		# UpdateMeBot acts on any substring hit, in this precedence.
		for term in ("subscribeme", "updateme", "subscribeall"):
			if term in lower:
				return term, KIND_COMMAND
	for term in SEARCH_TERMS.get(client, []):
		if term in lower:
			return term, KIND_PROSE
	return "unknown", KIND_PROSE
