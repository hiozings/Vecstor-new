"""Bounded SQL boundary lexer; semantic parsing belongs to the server."""


def scan(text):
    """Return (statement end offsets, final state, has SQL after last end)."""
    ends, state, quote, depth, code, i = [], 'normal', '', 0, False, 0
    while i < len(text):
        ch, pair = text[i], text[i:i + 2]
        if state == 'line':
            if ch == '\n':
                state = 'normal'
        elif state == 'block':
            if pair == '/*':
                depth += 1
                i += 1
            elif pair == '*/':
                depth -= 1
                i += 1
                if not depth:
                    state = 'normal'
        elif state == 'dollar':
            if text.startswith(quote, i):
                i += len(quote) - 1
                state = 'normal'
        elif state == 'quoted':
            if ch == quote:
                if i + 1 < len(text) and text[i + 1] == quote:
                    i += 1
                else:
                    state = 'normal'
        elif pair == '--':
            state = 'line'
            i += 1
        elif pair == '/*':
            state, depth = 'block', 1
            i += 1
        elif ch in "'\"":
            state, quote, code = 'quoted', ch, True
        elif ch == ';':
            ends.append((i + 1, code))
            code = False
        elif ch == '$' and (i == 0 or not (text[i - 1].isalnum() or text[i - 1] in '_$')):
            j = i + 1
            if j < len(text) and (text[j].isalpha() or text[j] == '_'):
                j += 1
                while j < len(text) and (text[j].isalnum() or text[j] == '_'):
                    j += 1
            if j < len(text) and text[j] == '$':
                quote, state = text[i:j + 1], 'dollar'
                i = j
            code = True
        elif not ch.isspace():
            code = True
        i += 1
    return ends, state, code


class StatementBuffer:
    def __init__(self, max_size=65536):
        self.max_size = max_size
        self.pending = ''

    def reset(self):
        self.pending = ''

    def feed(self, text):
        combined = self.pending + text
        if len(combined) > self.max_size:
            raise ValueError(f'Input exceeds {self.max_size} characters; clear or shorten it.')
        ends, state, code = scan(combined)
        start, statements = 0, []
        for end, has_code in ends:
            if has_code:
                statements.append(combined[start:end])
            start = end
        self.pending = combined[start:]
        if not code and state == 'normal':
            self.pending = ''
        return statements
