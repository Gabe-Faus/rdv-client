import unittest
from src.client.hangman import normalize_word, compute_word_hash, HangmanGame


class TestHangman(unittest.TestCase):

    def test_normalize_word_valid(self):
        self.assertEqual(normalize_word("banana"), "banana")
        self.assertEqual(normalize_word("Maçã"), "maca")
        self.assertEqual(normalize_word("   CÔMPUTADOR   "), "computador")
        self.assertEqual(normalize_word("áéíóúçãõâêîôû"), "aeioucaoaeiou")

    def test_normalize_word_invalid(self):
        with self.assertRaises(ValueError):
            normalize_word("ab")  # less than 3
        with self.assertRaises(ValueError):
            normalize_word("a" * 21)  # more than 20
        with self.assertRaises(ValueError):
            normalize_word("word-with-hyphen")
        with self.assertRaises(ValueError):
            normalize_word("word with spaces")
        with self.assertRaises(ValueError):
            normalize_word("123abc")

    def test_word_hash(self):
        h = compute_word_hash("banana")
        self.assertIsInstance(h, str)
        self.assertEqual(len(h), 64)
        self.assertEqual(h, compute_word_hash("BaNaNa"))

    def test_hangman_gameplay_victory_by_letters(self):
        game = HangmanGame("banana", initial_lives=6)
        self.assertEqual(game.masked, "______")
        self.assertEqual(game.word_length, 6)

        # Player 1 guesses 'a'
        acc, corr, reason = game.guess_letter("bob@CIC", "a")
        self.assertTrue(acc and corr)
        self.assertIsNone(reason)
        self.assertEqual(game.masked, "_a_a_a")
        self.assertEqual(game.get_score("bob@CIC"), 3)
        self.assertEqual(game.lives, 6)
        self.assertFalse(game.is_finished())

        # Wrong guess
        acc, corr, reason = game.guess_letter("carol@CIC", "e")
        self.assertTrue(acc)
        self.assertFalse(corr)
        self.assertEqual(game.masked, "_a_a_a")
        self.assertEqual(game.wrong_letters, ["e"])
        self.assertEqual(game.lives, 5)

        # Duplicate guess (already used letter)
        acc, corr, reason = game.guess_letter("bob@CIC", "a")
        self.assertFalse(acc)
        self.assertEqual(reason, "letter_already_used")

        acc, corr, reason = game.guess_letter("bob@CIC", "e")
        self.assertFalse(acc)
        self.assertEqual(reason, "letter_already_used")

        # Guess 'b'
        acc, corr, _ = game.guess_letter("carol@CIC", "b")
        self.assertTrue(acc and corr)
        self.assertEqual(game.masked, "ba_a_a")
        self.assertEqual(game.get_score("carol@CIC"), 1)

        # Guess 'n' -> finishes game
        acc, corr, _ = game.guess_letter("bob@CIC", "n")
        self.assertTrue(acc and corr)
        self.assertEqual(game.masked, "banana")
        self.assertEqual(game.get_score("bob@CIC"), 5)  # 3 ('a') + 2 ('n')
        self.assertTrue(game.is_won())
        self.assertEqual(game.result, "WON")
        self.assertEqual(game.winner, "bob@CIC")

    def test_hangman_gameplay_victory_by_word(self):
        game = HangmanGame("casa", initial_lives=6)
        acc, corr, _ = game.guess_letter("bob@CIC", "a")
        self.assertEqual(game.masked, "_a_a")
        self.assertEqual(game.get_score("bob@CIC"), 2)

        # Guess wrong word
        acc, corr, _ = game.guess_word("bob@CIC", "copa")
        self.assertTrue(acc)
        self.assertFalse(corr)
        self.assertEqual(game.lives, 5)
        self.assertEqual(game.masked, "_a_a")

        # Guess right word
        acc, corr, _ = game.guess_word("carol@CIC", "casa")
        self.assertTrue(acc and corr)
        self.assertEqual(game.masked, "casa")
        self.assertTrue(game.is_won())
        self.assertEqual(game.winner, "carol@CIC")
        # Remaining hidden letters were 2 ('c', 's'), so carol gets 2 points
        self.assertEqual(game.get_score("carol@CIC"), 2)

    def test_hangman_gameplay_loss(self):
        game = HangmanGame("uva", initial_lives=2)
        game.guess_letter("bob@CIC", "x")
        self.assertEqual(game.lives, 1)
        self.assertFalse(game.is_finished())

        game.guess_letter("bob@CIC", "z")
        self.assertEqual(game.lives, 0)
        self.assertTrue(game.is_lost())
        self.assertEqual(game.result, "LOST")
        self.assertIsNone(game.winner)


if __name__ == "__main__":
    unittest.main()
