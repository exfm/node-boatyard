"use strict";

var socket = io.connect('http://localhost:3000');



socket.on('message', function (data) {
    console.log('Got message', data);

    if(data.username !== lastMessageFrom){
        $('#stage').append('<hr />');
        lastMessageFrom = data.username;
    }

    $('#stage').append('<div class="message"><span class="name">'+data.username+'</span><span class="timestamp">'+data.timestamp+'</span><span class="message">'+data.message+'</span></div>');
    $('#stage').get(0).scrollTop = $('#stage').get(0).scrollHeight;
});

socket.on('notice', function (data) {
    console.log('Got server notice', data);
    $('#stage').append('<div class="notice">'+data.message+'</div>');
});

socket.on('add to roster', function(data){
   console.log('Got add to roster', data);
   $('#roster').append('<div id="'+data.username+'">'+data.username+'</div>');
});

socket.on('remove from roster', function(data){
    $('#'+data.username).remove();
    console.log('Got remove from roster', data);
});

socket.on('join ack', function(){
    $('#status-bar').html('');
    $('#chat').show();
    socket.emit('roster');
});

socket.on('roster list', function(list){

});


function layout(){
    var h = window.innerHeight - $('#send-message').height() - $('.navbar').height();
    $('#stage').css({'height': h - 20 +'px'});
}

$('window').on('resize', layout);
layout();
