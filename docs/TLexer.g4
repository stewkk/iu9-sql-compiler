lexer grammar TLexer;

channels { CommentsChannel }

tokens {
  DUMMY
}

Return: 'return';
Continue: 'continue';
Break: 'break';
If: 'if';
While: 'while';

INT: Digit+;
Digit: [0-9];

ID: LETTER (LETTER | '0'..'9')*;
fragment LETTER : [a-zA-Z\u0080-\u{10FFFF}];

LessThan: '<';
GreaterThan:  '>';
Assign: '=';
Equal: '==';
NotEqual: '!=';

Semicolon: ';';
Plus: '+';
Minus: '-';
Star: '*';
OpenPar: '(';
ClosePar: ')';
OpenCurly: '{';
CloseCurly: '}';

Comment : '#' ~[\r\n]* '\r'? '\n' -> channel(CommentsChannel);
WS: [ \t\r\n]+ -> channel(HIDDEN);
