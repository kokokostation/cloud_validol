import copy
import dataclasses
import enum
import logging
from typing import Any
from typing import Callable
from typing import Dict
from typing import Generator
from typing import List
from typing import Set

import networkx as nx
import pyparsing as pp


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


def make_atom_grammar():
    return pp.Word(pp.alphas, pp.alphanums + '-_')


def make_expression_grammar(
    allow_unknown_atoms: bool,
    atom_names: List[str],
    push: Callable[[ParsedToken], None],
):
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

    atom_expressions = [
        pp.Literal(atom_name) for atom_name in sorted(atom_names, key=lambda x: -len(x))
    ]
    if allow_unknown_atoms:
        atom_expressions.append(make_atom_grammar())

    timeseries = pp.Or(atom_expressions).setParseAction(
        lambda tokens: ParsedToken(type=TokenType.ATOM, value=tokens[0])
    )
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


def _parse_expression(
    allow_unknown_atoms: bool, atom_names: List[str], expression: str
) -> List[ParsedToken]:
    stack: List[ParsedToken] = []
    grammar = make_expression_grammar(allow_unknown_atoms, atom_names, stack.append)

    try:
        grammar.parseString(expression, parseAll=True)
    except pp.ParseBaseException as exc:
        logger.error('Failed to parse expression=%s: %s', expression, exc)

        raise ParseError(str(exc))

    return stack


def check_atom_name(name: str):
    grammar = make_atom_grammar()

    try:
        grammar.parseString(name, parseAll=True)
    except pp.ParseBaseException as exc:
        logger.error('Failed to parse atom name=%s: %s', name, exc)

        raise ParseError(str(exc))


def _get_nested_stack(
    allow_unknown_atoms: bool,
    library: ExpressionLibrary,
    expression: str,
    cache: Dict[str, List],
) -> List:
    cached_stack = cache.get(expression)
    if cached_stack is not None:
        return cached_stack

    atom_names = list(library.user_expressions) + library.basic_atoms
    stack: List = _parse_expression(allow_unknown_atoms, atom_names, expression)
    for index, token in enumerate(stack):
        if token.type != TokenType.ATOM:
            continue

        if token.value in library.basic_atoms:
            continue

        if allow_unknown_atoms and token.value not in library.user_expressions:
            continue

        atom_expression = library.user_expressions[token.value]

        stack[index] = _get_nested_stack(
            allow_unknown_atoms, library, atom_expression, cache
        )

    return stack


def flatten_list(list_: List) -> Generator[ParsedToken, None, None]:
    for item in list_:
        if isinstance(item, list):
            yield from flatten_list(item)
        else:
            yield item


def get_stack(
    allow_unknown_atoms: bool,
    expression: str,
    library: ExpressionLibrary,
) -> List[ParsedToken]:
    nested_stack = _get_nested_stack(allow_unknown_atoms, library, expression, {})

    return list(flatten_list(nested_stack))


def get_dependencies(stack: List[ParsedToken]) -> Set[str]:
    return {token.value for token in stack if token.type is TokenType.ATOM}


def build_atom_graph(user_expressions: Dict[str, str]) -> nx.DiGraph:
    graph = nx.DiGraph()

    for atom_name, expression in user_expressions.items():
        stack = _parse_expression(
            allow_unknown_atoms=True,
            atom_names=[],
            expression=expression,
        )

        graph.add_node(atom_name)
        for dep_atom_name in get_dependencies(stack):
            graph.add_edge(atom_name, dep_atom_name)

    return graph


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
