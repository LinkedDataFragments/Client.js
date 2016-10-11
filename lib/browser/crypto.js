/*! @license MIT ©2015-2016 Ruben Verborgh, Ghent University - imec */
/* Browser replacement for a subset of crypto. */

exports.createHash = function () {
  var contents;
  return {
    update: function (c) { contents ? (contents += c) : (contents = c); },
    digest: function ()  { return sha1(contents); },
  };
};

/*! @license MIT ©2002-2014 Chris Veness */
/* SHA-1 implementation */

// constants [§4.2.1]
var K = [0x5a827999, 0x6ed9eba1, 0x8f1bbcdc, 0xca62c1d6];
var pow2to35 = Math.pow(2, 35);

/**
 * Generates SHA-1 hash of string.
 *
 * @param   {string} msg - (Unicode) string to be hashed.
 * @returns {string} Hash of msg as hex character string.
 */
function sha1(msg) {
  // PREPROCESSING
  msg += '\u0080';  // add trailing '1' bit (+ 0's padding) to string [§5.1.1]

  // convert string msg into 512-bit/16-integer blocks arrays of ints [§5.2.1]
  var length = msg.length;
  var l = length / 4 + 2; // length (in 32-bit integers) of msg + ‘1’ + appended length
  var N = ~~((l + 15) / 16);  // number of 16-integer-blocks required to hold 'l' ints
  var M = new Array(N);

  var i, j, index;
  for (i = 0, index = 0; i < N; i++) {
    M[i] = new Array(16);
    for (j = 0; j < 16; j++, index++) {  // encode 4 chars per integer, big-endian encoding
      M[i][j] =   (index < length ? msg.charCodeAt(index) << 24 : 0) |
                (++index < length ? msg.charCodeAt(index) << 16 : 0) |
                (++index < length ? msg.charCodeAt(index) <<  8 : 0) |
                (++index < length ? msg.charCodeAt(index)       : 0);
    }
  }
  // add length (in bits) into final pair of 32-bit integers (big-endian) [§5.1.1]
  // note: most significant word would be (len-1)*8 >>> 32, but since JS converts
  // bitwise-op args to 32 bits, we need to simulate this by arithmetic operators
  M[N - 1][14] = ~~((msg.length - 1) / pow2to35);
  M[N - 1][15] = ((msg.length - 1) * 8) & 0xffffffff;

  // set initial hash value [§5.3.1]
  var H0 = 0x67452301, H1 = 0xefcdab89, H2 = 0x98badcfe, H3 = 0x10325476, H4 = 0xc3d2e1f0;

  // HASH COMPUTATION [§6.1.2]
  var W = new Array(80), a, b, c, d, e, t;
  for (i = 0; i < N; i++) {
    // 1 - prepare message schedule 'W'
    for (t = 0;  t < 16; t++) W[t] = M[i][t];
    for (t = 16; t < 80; t++) W[t] = rotl(W[t - 3] ^ W[t - 8] ^ W[t - 14] ^ W[t - 16], 1);

    // 2 - initialise five working variables a, b, c, d, e with previous hash value
    a = H0, b = H1, c = H2, d = H3, e = H4;

    // 3 - main loop
    for (t = 0; t < 80; t++) {
      var s = ~~(t / 20); // seq for blocks of 'f' functions and 'K' constants
      var T = (rotl(a, 5) + f(s, b, c, d) + e + K[s] + W[t]) & 0xffffffff;
      e = d;
      d = c;
      c = rotl(b, 30);
      b = a;
      a = T;
    }

    // 4 - compute the new intermediate hash value (note 'addition modulo 2^32')
    H0 = (H0 + a) & 0xffffffff;
    H1 = (H1 + b) & 0xffffffff;
    H2 = (H2 + c) & 0xffffffff;
    H3 = (H3 + d) & 0xffffffff;
    H4 = (H4 + e) & 0xffffffff;
  }

  return toHexStr(H0) + toHexStr(H1) + toHexStr(H2) + toHexStr(H3) + toHexStr(H4);
}

/**
 * Function 'f' [§4.1.1].
 */
function f(s, x, y, z)  {
  switch (s) {
  case 0:
    return (x & y) ^ (~x & z);           // Ch()
  case 1:
    return  x ^ y  ^  z;                 // Parity()
  case 2:
    return (x & y) ^ (x & z) ^ (y & z);  // Maj()
  case 3:
    return  x ^ y  ^  z;                 // Parity()
  }
}

/**
 * Rotates left (circular left shift) value x by n positions [§3.2.5].
 */
function rotl(x, n) {
  return (x << n) | (x >>> (32 - n));
}

/**
 * Hexadecimal representation of a number.
 */
function toHexStr(n) {
  // note can't use toString(16) as it is implementation-dependant,
  // and in IE returns signed numbers when used on full words
  var s = '', v;
  for (var i = 7; i >= 0; i--) { v = (n >>> (i * 4)) & 0xf; s += v.toString(16); }
  return s;
}
