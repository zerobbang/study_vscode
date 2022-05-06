const boxEl = document.querySelector(".box");
console.log(boxEl);

boxEl.addEventListener('click', function() {
    console.log('Click!');
    boxEl.classList.add('active');

    
    //const hasActive = boxEl.classList.contains('active');
    //console.log(hasActive); // true
    
   let hasActive = boxEl.classList.contains('active');
   console.log(hasActive);

   boxEl.classList.remove('active');
   hasActive = boxEl.classList.contains('active');
   console.log(hasActive);
});