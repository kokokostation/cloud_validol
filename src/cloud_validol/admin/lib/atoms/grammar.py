import copy
import dataclasses
import enum
import logging
import pyparsing as pp
from typing import Any
from typing import Callable
from typing import Dict
from typing import Generator
from typing import List


logger = logging.getLogger(__name__)


class BaseError(Exception):
    pass


class ParseError(BaseError):
    pass


class TokenType(enum.Enum):
    NUMBER = 0
    ARITHMETIC_OPERATION = 1
    ATOM = 2


@dataclasses.dataclass
class ParsedToken:
    type: TokenType
    value: Any


@dataclasses.dataclass
class ExpressionLibrary:
    user_expressions: Dict[str, str]
    basic_atoms: List[str]


def make_grammar(atom_names: List[str], push: Callable[[ParsedToken], None]):
    push_first = lambda tokens: push(tokens[0])

    point = pp.Literal('.')
    lpar = pp.Literal('(')
    rpar = pp.Literal(')')
    add_op = pp.oneOf(['+', '-']).setParseAction(
        lambda tokens: ParsedToken(type=TokenType.ARITHMETIC_OPERATION, value=tokens[0])
    )
    mult_op = pp.oneOf(['*', '/']).setParseAction(
        lambda tokens: ParsedToken(type=TokenType.ARITHMETIC_OPERATION, value=tokens[0])
    )

    timeseries = pp.Or(
        [
            pp.Literal(atom_name)
            for atom_name in sorted(atom_names, key=lambda x: -len(x))
        ]
    ).setParseAction(lambda tokens: ParsedToken(type=TokenType.ATOM, value=tokens[0]))
    number = pp.Combine(
        pp.Word(pp.nums) + pp.Optional(point + pp.Optional(pp.Word(pp.nums)))
    ).setParseAction(
        lambda tokens: ParsedToken(type=TokenType.NUMBER, value=float(tokens[0]))
    )

    expr = pp.Forward()

    token = (number | timeseries).setParseAction(push_first)
    atom = token | pp.Group(lpar + expr + rpar)
    term = atom + pp.ZeroOrMore((mult_op + atom).setParseAction(push_first))
    expr << term + pp.ZeroOrMore((add_op + term).setParseAction(push_first))

    return expr


def parse_expression(expression: str, library: ExpressionLibrary) -> List[ParsedToken]:
    atom_names = list(library.user_expressions) + library.basic_atoms

    stack: List[ParsedToken] = []
    grammar = make_grammar(atom_names, stack.append)

    try:
        grammar.parseString(expression, parseAll=True)
    except pp.ParseBaseException as exc:
        logger.error('Failed to parse expression=%s: %s', expression, exc)

        raise ParseError(str(exc))

    return stack


def _get_nested_stack(
    expression: str,
    library: ExpressionLibrary,
    cache: Dict[str, List],
) -> List:
    cached_stack = cache.get(expression)
    if cached_stack is not None:
        return cached_stack

    stack: List = parse_expression(expression, library)
    for index, token in enumerate(stack):
        if token.type != TokenType.ATOM:
            continue

        if token.value in library.basic_atoms:
            continue

        atom_expression = library.user_expressions[token.value]

        stack[index] = _get_nested_stack(atom_expression, library, cache)

    return stack


def flatten_list(list_: List) -> Generator[ParsedToken, None, None]:
    for item in list_:
        if isinstance(item, list):
            yield from flatten_list(item)
        else:
            yield item


def get_stacks(
    expressions: List[str],
    library: ExpressionLibrary,
) -> List[List[ParsedToken]]:
    stacks = []
    for expression in expressions:
        nested_stack = _get_nested_stack(expression, library, {})

        stacks.append(list(flatten_list(nested_stack)))

    return stacks


def get_dependencies(stack: List[ParsedToken]) -> List[str]:
    return list({token.value for token in stack if token.type is TokenType.ATOM})


def _render_stack_helper(stack: List[ParsedToken]) -> str:
    token = stack.pop()

    if token.type is TokenType.ATOM:
        return token.value
    elif token.type is TokenType.NUMBER:
        return str(token.value)
    elif token.type is TokenType.ARITHMETIC_OPERATION:
        left_operand = _render_stack_helper(stack)
        right_operand = _render_stack_helper(stack)

        return f'({left_operand} {token.value} {right_operand})'
    else:
        raise ValueError()


def render_stack(stack: List[ParsedToken]) -> str:
    return _render_stack_helper(copy.deepcopy(stack))
