from __future__ import annotations
import hashlib
import unicodedata
from typing import List, Dict, Optional, Tuple


def normalize_word(word: str) -> str:
    """
    Normalizes a word: strips whitespace, converts to lowercase,
    removes diacritics/accents, and verifies that only 'a'-'z' remain,
    with length between 3 and 20.
    Raises ValueError if invalid.
    """
    if not isinstance(word, str):
        raise ValueError("Word must be a string")
    
    # Strip whitespace and convert to lowercase
    w = word.strip().lower()
    
    # Remove accents using NFD decomposition
    nfkd_form = unicodedata.normalize("NFD", w)
    w_clean = "".join([c for c in nfkd_form if unicodedata.category(c) != "Mn"])
    
    if len(w_clean) < 3 or len(w_clean) > 20:
        raise ValueError(f"Word length must be between 3 and 20 characters (got {len(w_clean)})")
    
    if not w_clean.isalpha() or not all('a' <= c <= 'z' for c in w_clean):
        raise ValueError("Word must contain only ASCII letters (a-z) after accent removal")
        
    return w_clean


def compute_word_hash(word: str) -> str:
    """
    Computes lowercase hexadecimal SHA-256 of the normalized word.
    """
    normalized = normalize_word(word)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


class HangmanGame:
    """
    Pure logic for the Hangman game (PyP2P-Forca).
    Network-independent, fully unit-testable.
    """

    def __init__(self, secret_word: str, initial_lives: int = 6):
        self.secret_word: str = normalize_word(secret_word)
        self.word_hash: str = hashlib.sha256(self.secret_word.encode("utf-8")).hexdigest()
        self.initial_lives: int = initial_lives
        self.lives: int = initial_lives
        self.wrong_letters: List[str] = []
        self.masked: str = "_" * len(self.secret_word)
        self.scores: Dict[str, int] = {}
        self.winner: Optional[str] = None
        self.finished: bool = False
        self.result: Optional[str] = None  # "WON", "LOST", "ABORTED"

    @property
    def word_length(self) -> int:
        return len(self.secret_word)

    def is_won(self) -> bool:
        return "_" not in self.masked

    def is_lost(self) -> bool:
        return self.lives <= 0

    def is_finished(self) -> bool:
        return self.finished or self.is_won() or self.is_lost()

    def get_score(self, player_id: str) -> int:
        return self.scores.get(player_id, 0)

    def add_score(self, player_id: str, points: int) -> None:
        self.scores[player_id] = self.scores.get(player_id, 0) + points

    def guess_letter(self, player_id: str, letter: str) -> Tuple[bool, Optional[bool], Optional[str]]:
        """
        Processes a letter guess by a player.
        Returns: (accepted, correct, reason)
        - If accepted is False, correct is None, reason is error code.
        - If accepted is True, correct is bool (True or False), reason is None.
        """
        if self.is_finished():
            return False, None, "game_not_running"

        if not letter or not isinstance(letter, str):
            return False, None, "invalid_letter"

        # Normalize letter
        letter_norm = letter.strip().lower()
        if len(letter_norm) != 1 or not ('a' <= letter_norm <= 'z'):
            return False, None, "invalid_letter"

        # Check if already used
        if letter_norm in self.wrong_letters or letter_norm in self.masked:
            return False, None, "letter_already_used"

        if letter_norm in self.secret_word:
            # Correct guess
            # Reveal all occurrences
            new_masked_chars = []
            revealed_count = 0
            for idx, c in enumerate(self.secret_word):
                if c == letter_norm:
                    new_masked_chars.append(c)
                    revealed_count += 1
                else:
                    new_masked_chars.append(self.masked[idx])
            self.masked = "".join(new_masked_chars)
            self.add_score(player_id, revealed_count)

            if self.is_won():
                self.finished = True
                self.result = "WON"
                self.winner = player_id

            return True, True, None
        else:
            # Wrong guess
            self.wrong_letters.append(letter_norm)
            self.lives -= 1
            if self.is_lost():
                self.finished = True
                self.result = "LOST"
                self.winner = None

            return True, False, None

    def guess_word(self, player_id: str, word: str) -> Tuple[bool, Optional[bool], Optional[str]]:
        """
        Processes a full word guess by a player.
        Returns: (accepted, correct, reason)
        """
        if self.is_finished():
            return False, None, "game_not_running"

        try:
            guessed_word_norm = normalize_word(word)
        except ValueError:
            return False, None, "invalid_word"

        if guessed_word_norm == self.secret_word:
            # Correct word guess!
            # Count hidden letters to award score
            hidden_count = self.masked.count("_")
            self.add_score(player_id, hidden_count)
            self.masked = self.secret_word
            self.finished = True
            self.result = "WON"
            self.winner = player_id
            return True, True, None
        else:
            # Incorrect word guess: costs 1 life, reveals nothing
            self.lives -= 1
            if self.is_lost():
                self.finished = True
                self.result = "LOST"
                self.winner = None
            return True, False, None

    def abort(self, reason: str = "host_shutdown") -> None:
        self.finished = True
        self.result = "ABORTED"
        self.winner = None
