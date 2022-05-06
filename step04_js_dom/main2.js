const boxEls = document.querySelectorAll('.box');
console.log(boxEls);

boxEls.forEach(function (boxEl, index) {
    // div 클래스명을 box order-1
    boxEl.classList.add(`order-${index + 1}`);
    console.log(boxEl, index);
    console.log(boxEl.textContent);
    boxEl.textContent = "안녕";
    console.log(boxEl.textContent);
});