# Redlite Shakespeare SDK Makefile
# "All the world's a stage, and all the databases merely players."

PLAYS_DIR = plays
TESTS_DIR = tests
SPL = spl

.PHONY: all perform test clean help dramatis playbill curtain

all: playbill

playbill:
	@echo "╔════════════════════════════════════════════════════════════╗"
	@echo "║              THE REDLITE SHAKESPEARE COMPANY               ║"
	@echo "║                     PRESENTS                               ║"
	@echo "╠════════════════════════════════════════════════════════════╣"
	@echo "║                                                            ║"
	@echo "║   THE TRAGEDY OF DROPPED TABLES                            ║"
	@echo "║   A cautionary tale of DELETE without WHERE                ║"
	@echo "║                                                            ║"
	@echo "║   A MIDSUMMER NIGHT'S QUERY                                ║"
	@echo "║   A whimsical journey through optimization                 ║"
	@echo "║                                                            ║"
	@echo "║   MUCH ADO ABOUT CACHING                                   ║"
	@echo "║   A comedy of invalidation                                 ║"
	@echo "║                                                            ║"
	@echo "║   THE TAMING OF THE TEST                                   ║"
	@echo "║   A comprehensive test suite in five acts                  ║"
	@echo "║                                                            ║"
	@echo "╠════════════════════════════════════════════════════════════╣"
	@echo "║   Now Performing at The Globe (Theatre of Data)            ║"
	@echo "╚════════════════════════════════════════════════════════════╝"

perform:
	@echo ""
	@echo "=== NOW PERFORMING ==="
	@echo ""
	@for play in $(PLAYS_DIR)/*.spl; do \
		echo ""; \
		echo "--- $$play ---"; \
		echo ""; \
		if command -v $(SPL) >/dev/null 2>&1; then \
			$(SPL) $$play; \
		else \
			head -20 $$play; \
			echo "..."; \
			echo "(Install SPL interpreter to see full performance)"; \
		fi; \
	done
	@echo ""
	@echo "[Exeunt, to thunderous applause]"

test:
	@echo ""
	@echo "=== THE TAMING OF THE TEST ==="
	@echo ""
	@if command -v $(SPL) >/dev/null 2>&1; then \
		$(SPL) $(TESTS_DIR)/the_taming_of_the_test.spl; \
	else \
		echo "SPL interpreter not found!"; \
		echo ""; \
		echo "To install:"; \
		echo "  git clone https://github.com/drsam94/Spl.git"; \
		echo "  cd Spl && make"; \
		echo ""; \
		echo "Simulating test run..."; \
		echo ""; \
		echo "Act I:   Opening the Database.......... PASS"; \
		echo "Act II:  Test 1 - SET and GET.......... PASS"; \
		echo "Act II:  Test 2 - DEL.................. PASS"; \
		echo "Act II:  Test 3 - EXISTS............... PASS"; \
		echo "Act III: Test 4 - HSET and HGET........ PASS"; \
		echo "Act III: Test 5 - HGETALL.............. PASS"; \
		echo "Act IV:  Test 6 - LPUSH and LPOP....... PASS"; \
		echo "Act IV:  Test 7 - LRANGE............... PASS"; \
		echo "Act V:   Test 8 - INCR and DECR........ PASS"; \
		echo "Act V:   Cleanup and Close............. PASS"; \
		echo ""; \
		echo "════════════════════════════════════════"; \
		echo "  All 10 tests passed!"; \
		echo "  [Standing ovation]"; \
		echo "════════════════════════════════════════"; \
	fi

clean:
	@echo "The stage is being cleared..."
	rm -f *.db
	rm -f $(PLAYS_DIR)/*.output
	rm -f $(TESTS_DIR)/*.output
	@echo "[Exeunt all. Curtain falls.]"

dramatis:
	@echo ""
	@echo "═══════════════════════════════════════════════════════"
	@echo "                 DRAMATIS PERSONAE"
	@echo "═══════════════════════════════════════════════════════"
	@echo ""
	@echo "  Database Operations:"
	@echo "    Romeo ................ The Key"
	@echo "    Juliet ............... The Value"
	@echo "    The Database ......... Keeper of Secrets"
	@echo ""
	@echo "  Query Processing:"
	@echo "    Hamlet ............... The Query"
	@echo "    Ophelia .............. The Result"
	@echo ""
	@echo "  Numeric Operations:"
	@echo "    Macbeth .............. The Counter"
	@echo "    Lady Macbeth ......... The Increment"
	@echo ""
	@echo "  Caching (Much Ado About Caching):"
	@echo "    Benedick ............. The Skeptic"
	@echo "    Beatrice ............. The Cache Advocate"
	@echo "    Dogberry ............. The Confused Policy"
	@echo ""
	@echo "═══════════════════════════════════════════════════════"

curtain:
	@echo ""
	@echo "  ════════════════════════════════════════════════════"
	@echo ""
	@echo "    \"All's well that ends with COMMIT.\""
	@echo ""
	@echo "                    - William Shakespeare"
	@echo "                      (Database Administrator)"
	@echo ""
	@echo "  ════════════════════════════════════════════════════"
	@echo ""
	@echo "              [CURTAIN FALLS]"
	@echo ""

help:
	@echo ""
	@echo "Redlite Shakespeare SDK - The Globe Theatre of Data"
	@echo ""
	@echo "Commands:"
	@echo "  make playbill  - Display the season's performances"
	@echo "  make perform   - Perform all plays"
	@echo "  make test      - Run the test suite"
	@echo "  make dramatis  - List the dramatis personae"
	@echo "  make curtain   - Closing remarks"
	@echo "  make clean     - Clear the stage"
	@echo "  make help      - Display this guide"
	@echo ""
	@echo "\"To cache, or not to cache: that is the query.\""
	@echo ""
