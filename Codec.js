// v3 to v4 compatibility wrapper
function decodeUplink(input) {
	return {
		data: Decode(input.fPort, input.bytes, input.variables)
	};
}

function intFromBytes( x ){
    var val = 0;
    for (var i = 0; i < x.length; ++i) {
        val += x[i];
        if (i < x.length-1) {
            val = val << 8;
        }
    }
    return val;
}


function bytesToFloat(bytes) {
  var bits = bytes[0]<<24 | bytes[1]<<16 | bytes[2]<<8 | bytes[3];
  var sign = (bits>>>31 === 0) ? 1.0 : -1.0;
  var e = bits>>>23 & 0xff;
  var m = (e === 0) ? (bits & 0x7fffff)<<1 : (bits & 0x7fffff) | 0x800000;
  var f = sign * m * Math.pow(2, e - 150);
  return parseFloat(f);
}

// Decode decodes an array of bytes into an object.
//  - fPort contains the LoRaWAN fPort number
//  - bytes is an array of bytes, e.g. [225, 230, 255, 0]
//  - variables contains the device variables e.g. {"calibration": "3.5"} (both the key / value are of type string)
// The function must return an object, e.g. {"temperature": 22.5}
function Decode(fPort, bytes, variables) {
      var decoded = {};

if ( bytes[4] == 01) {

  decoded.SERIAL_NO = intFromBytes(bytes.slice(0, 4));   // Serial number
  decoded.INSTANT_WATT = (bytesToFloat(bytes.slice(06, 10))); // Instant W read
  decoded.KWH = (bytesToFloat(bytes.slice(10, 14))); //  Total Kwh
  decoded.CURRENT = (bytesToFloat(bytes.slice(14, 18)));  // Always zero?
  decoded.KWH_2 = (bytesToFloat(bytes.slice(18, 22)));  /// Absolute Kwh
  decoded.KVArh_imp  = (bytesToFloat(bytes.slice(22, 26))); // KVArh Imported
  decoded.KVArh_exp = (bytesToFloat(bytes.slice(26, 30))); // kVArh Exported
  decoded.KVArh_tot = (bytesToFloat(bytes.slice(30, 34))); // KVArh Total
}


return decoded;
}
