// print : Good Morning
console.log("Good Morning");

// 변수명 작성 스타일
// camelCase, numOne, strOne

// const 변수 지정
const myName = 'zero';
const email = 'hi@gmail.com';
const hello = `안녕 ${myName}!`;

console.log(myName);
console.log(email);
console.log(hello);


// 숫자
const number = 123;
console.log(number);

// Boolean
let checked = true;
console.log(checked);


let abc;
console.log(abc);
// Undefined 할당이 되지 않았다.

// NULL
let name = null;
console.log(name);
// 재 할당
name = 'zero';
console.log(name);
// const가 아닌 let 을 사용했기 때문에 재 할당이 가능

// 파이선 딕셔너리와 같은 역할
// key-value 값
const user = {
    name : 'zero',
    age : 24,
    isValid : true
};

console.log(user.name);
console.log(user.age);
console.log(user.isValid);
console.log(user.city);      // 할당 되지 않은 변수명 -> undefined


// 사칙연산
const a = 2;
const b = 5;
console.log(a+b);
console.log(a-b);
console.log(a*b);
console.log(a/b);

// 함수의 선언과 호출
// ;안해도 될것이다.
function helloFunc() {
    console.log(1234);
}

helloFunc();

// 기명 함수
function returnFunc() {
    return 123;
}

const fun_result = returnFunc();
console.log(fun_result);

function sum(a,b) {
    return a + b;
}

const sum_a = sum(1,2);
console.log(sum_a);
const sum_b = sum(4,5);
console.log(sum_b);

// anomyous 함수 (익명 함수)
const world = function(){
    console.log("I am Qeen.");
}
world();

// 조건문
const isDone_a = true;
if (isDone_a) {
    console.log('done');
} else {
    console.log('Not yet');
}

const isDone_b = false;
if (isDone_b) {
    console.log('done');
} else {
    console.log('Not yet');
}

