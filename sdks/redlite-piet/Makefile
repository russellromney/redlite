# Redlite Piet SDK Makefile
# Where Code Becomes Art

PIET = npiet
ART_DIR = art
TESTS_DIR = tests

.PHONY: all gallery exhibit critique clean help vernissage

all: vernissage

vernissage:
	@echo ""
	@echo "╔══════════════════════════════════════════════════════════════╗"
	@echo "║                                                              ║"
	@echo "║              THE REDLITE PIET GALLERY                        ║"
	@echo "║                                                              ║"
	@echo "║          ~ Where Database Operations Become Art ~            ║"
	@echo "║                                                              ║"
	@echo "╠══════════════════════════════════════════════════════════════╣"
	@echo "║                                                              ║"
	@echo "║   🎨 Now Exhibiting:                                         ║"
	@echo "║                                                              ║"
	@echo "║   • \"The Opening\"           database_open.png               ║"
	@echo "║   • \"Persistence\"           set_value.png                   ║"
	@echo "║   • \"Retrieval\"             get_value.png                   ║"
	@echo "║   • \"The Many Becoming One\" hash_operations.png             ║"
	@echo "║                                                              ║"
	@echo "╠══════════════════════════════════════════════════════════════╣"
	@echo "║                                                              ║"
	@echo "║   Gallery Hours: 24/7 (npiet required)                       ║"
	@echo "║   Admission: Free (art should be accessible)                 ║"
	@echo "║                                                              ║"
	@echo "╚══════════════════════════════════════════════════════════════╝"
	@echo ""

gallery:
	@echo "Opening gallery in browser..."
	@if [ -f gallery.html ]; then \
		open gallery.html 2>/dev/null || xdg-open gallery.html 2>/dev/null || \
		echo "Please open gallery.html in your browser"; \
	else \
		echo "gallery.html not found"; \
	fi

exhibit:
	@echo ""
	@echo "=== EXECUTING ARTWORKS ==="
	@echo ""
	@if command -v $(PIET) >/dev/null 2>&1; then \
		for art in $(ART_DIR)/*.png; do \
			echo "--- Performing: $$art ---"; \
			$(PIET) $$art; \
			echo ""; \
		done; \
	else \
		echo "npiet interpreter not found!"; \
		echo ""; \
		echo "To install:"; \
		echo "  brew install npiet"; \
		echo "  # or"; \
		echo "  apt-get install npiet"; \
		echo ""; \
		echo "Without the interpreter, you can still:"; \
		echo "  - View gallery.html in a browser"; \
		echo "  - Appreciate the art visually"; \
		echo "  - Contemplate the meaning of color"; \
	fi

critique:
	@echo ""
	@echo "=== ART CRITIC (TEST SUITE) ==="
	@echo ""
	@echo "The critic examines each piece..."
	@echo ""
	@if command -v $(PIET) >/dev/null 2>&1; then \
		echo "database_open.png:"; \
		$(PIET) $(ART_DIR)/database_open.png && echo "  Verdict: Masterpiece!" || echo "  Verdict: Needs work..."; \
		echo ""; \
		echo "set_value.png:"; \
		$(PIET) $(ART_DIR)/set_value.png && echo "  Verdict: Brilliant!" || echo "  Verdict: Derivative..."; \
		echo ""; \
		echo "get_value.png:"; \
		$(PIET) $(ART_DIR)/get_value.png && echo "  Verdict: Evocative!" || echo "  Verdict: Pedestrian..."; \
	else \
		echo "Simulating critique..."; \
		echo ""; \
		echo "database_open.png:"; \
		echo "  \"A bold statement on connection and belonging.\""; \
		echo "  Verdict: Masterpiece!"; \
		echo ""; \
		echo "set_value.png:"; \
		echo "  \"The interplay of key and value speaks to the human condition.\""; \
		echo "  Verdict: Brilliant!"; \
		echo ""; \
		echo "get_value.png:"; \
		echo "  \"From darkness, data emerges. Profound.\""; \
		echo "  Verdict: Evocative!"; \
		echo ""; \
		echo "hash_operations.png:"; \
		echo "  \"Chaos and order in perfect balance.\""; \
		echo "  Verdict: Transcendent!"; \
	fi
	@echo ""
	@echo "The exhibition receives a standing ovation. 👏"

clean:
	@echo "The gallery is being closed for renovation..."
	rm -f $(ART_DIR)/*.output
	rm -f $(TESTS_DIR)/*.output
	@echo "The walls are bare. The art awaits its next showing."

help:
	@echo ""
	@echo "Redlite Piet SDK - The Gallery"
	@echo ""
	@echo "Commands:"
	@echo "  make vernissage - Display gallery information"
	@echo "  make gallery    - Open gallery.html in browser"
	@echo "  make exhibit    - Execute all artworks"
	@echo "  make critique   - Run the test suite (art criticism)"
	@echo "  make clean      - Close the gallery"
	@echo "  make help       - Display this guide"
	@echo ""
	@echo "Color Reference:"
	@echo "  🔴 Red     - Push operations"
	@echo "  🟡 Yellow  - Transition states"
	@echo "  🟢 Green   - Execute operations"
	@echo "  🔵 Blue    - Database syscalls"
	@echo "  🟣 Magenta - Result handling"
	@echo "  ⬛ Black   - Barriers (no entry)"
	@echo "  ⬜ White   - Free space (no operation)"
	@echo ""
	@echo "\"In art, as in databases, every pixel matters.\""
	@echo ""
