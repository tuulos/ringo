
jQuery.fn.log = function (msg) {
      console.log("%s: %o", msg, this);
      return this;
 };

$(document).ready(function(){
        $.getJSON("/mon/ring/nodes", update_ring);
        $.ajaxSetup({
                cache: false, 
        })
});

function nodename(node){
        s = node.split("@");
        return [s[0].substr(6), s[1]];
}

function update_ring(ringdata)
{
        $("#ringlist").html($.map(ringdata, function(node, i){
                mem = Math.round(node.disk[3] / (1024 * 1024)) + "MB";
                warn = "";
                if (!node.ok){ warn = "warn"; }
                N = $.create("div", {"class": "nodenfo"}, [
                        node.disk[2] + " / " + node.disk[0] + " / " +
                        node.disk[1] + " / " + mem]);
                
                nextn = nodename(node.neighbors[1]);
                thisn = nodename(node.node);
                T = $.create("div", {}, [$.create("span", {"class": "host"}, [thisn[1] + ":"]),
                                         $.create("span", {"class": "hexid"},
                                                [thisn[0] + " \u279D " + nextn[0]])]);
                return $.create("div", {"class": "listitem " + warn}, [T, N]);
        }));
        
        setTimeout(function(){
                $.getJSON("/mon/ring/nodes", update_ring);
        }, 10000);
}



