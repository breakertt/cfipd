package main

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"net/netip"
	"time"
)

// CloudflareSpeedTest's bundled ip.txt: official ips-v4 plus 104.28.0.0/14,
// with 172.64.0.0/13 split into finer ranges (battle-tested for CN use).
var cfRanges = []string{
	"173.245.48.0/20",
	"103.21.244.0/22",
	"103.22.200.0/22",
	"103.31.4.0/22",
	"141.101.64.0/18",
	"108.162.192.0/18",
	"190.93.240.0/20",
	"188.114.96.0/20",
	"197.234.240.0/22",
	"198.41.128.0/17",
	"162.158.0.0/15",
	"104.16.0.0/12",
	"172.64.0.0/17",
	"172.64.128.0/18",
	"172.64.192.0/19",
	"172.64.224.0/22",
	"172.64.229.0/24",
	"172.64.230.0/23",
	"172.64.232.0/21",
	"172.64.240.0/21",
	"172.64.248.0/21",
	"172.65.0.0/16",
	"172.66.0.0/16",
	"172.67.0.0/16",
	"131.0.72.0/22",
}

// CloudflareSpeedTest's bundled ipv6.txt. CF edge answers on arbitrary
// addresses inside announced prefixes, so random sampling works.
var cfRanges6 = []string{
	"2400:cb00:2049::/48", "2400:cb00:f00e::/48",
	"2606:4700::/32",
	"2606:4700:10::/48", "2606:4700:130::/48",
	"2606:4700:3000::/48", "2606:4700:3001::/48", "2606:4700:3002::/48",
	"2606:4700:3003::/48", "2606:4700:3004::/48", "2606:4700:3005::/48",
	"2606:4700:3006::/48", "2606:4700:3007::/48", "2606:4700:3008::/48",
	"2606:4700:3009::/48", "2606:4700:3010::/48", "2606:4700:3011::/48",
	"2606:4700:3012::/48", "2606:4700:3013::/48", "2606:4700:3014::/48",
	"2606:4700:3015::/48", "2606:4700:3016::/48", "2606:4700:3017::/48",
	"2606:4700:3018::/48", "2606:4700:3019::/48", "2606:4700:3020::/48",
	"2606:4700:3021::/48", "2606:4700:3022::/48", "2606:4700:3023::/48",
	"2606:4700:3024::/48", "2606:4700:3025::/48", "2606:4700:3026::/48",
	"2606:4700:3027::/48", "2606:4700:3028::/48", "2606:4700:3029::/48",
	"2606:4700:3030::/48", "2606:4700:3031::/48", "2606:4700:3032::/48",
	"2606:4700:3033::/48", "2606:4700:3034::/48", "2606:4700:3035::/48",
	"2606:4700:3036::/48", "2606:4700:3037::/48", "2606:4700:3038::/48",
	"2606:4700:3039::/48",
	"2606:4700:a0::/48", "2606:4700:a1::/48", "2606:4700:a8::/48",
	"2606:4700:a9::/48", "2606:4700:a::/48", "2606:4700:b::/48",
	"2606:4700:c::/48", "2606:4700:d0::/48", "2606:4700:d1::/48",
	"2606:4700:d::/48", "2606:4700:e0::/48", "2606:4700:e1::/48",
	"2606:4700:e2::/48", "2606:4700:e3::/48", "2606:4700:e4::/48",
	"2606:4700:e5::/48", "2606:4700:e6::/48", "2606:4700:e7::/48",
	"2606:4700:e::/48", "2606:4700:f1::/48", "2606:4700:f2::/48",
	"2606:4700:f3::/48", "2606:4700:f4::/48", "2606:4700:f5::/48",
	"2606:4700:f::/48",
	"2803:f800:50::/48", "2803:f800:51::/48",
	"2a06:98c1:3100::/48", "2a06:98c1:3101::/48", "2a06:98c1:3102::/48",
	"2a06:98c1:3103::/48", "2a06:98c1:3104::/48", "2a06:98c1:3105::/48",
	"2a06:98c1:3106::/48", "2a06:98c1:3107::/48", "2a06:98c1:3108::/48",
	"2a06:98c1:3109::/48", "2a06:98c1:310a::/48", "2a06:98c1:310b::/48",
	"2a06:98c1:310c::/48", "2a06:98c1:310d::/48", "2a06:98c1:310e::/48",
	"2a06:98c1:310f::/48", "2a06:98c1:3120::/48", "2a06:98c1:3121::/48",
	"2a06:98c1:3122::/48", "2a06:98c1:3123::/48", "2a06:98c1:3200::/48",
	"2a06:98c1:50::/48", "2a06:98c1:51::/48", "2a06:98c1:54::/48",
	"2a06:98c1:58::/48",
}

type PrefixStat struct {
	Prefix    string    `json:"prefix"`
	Probes    int       `json:"probes"`
	OK        int       `json:"ok"`
	ColoOK    int       `json:"colo_ok"`
	RTTSum    float64   `json:"rtt_sum"`
	RTTCount  int       `json:"rtt_count"`
	LastProbe time.Time `json:"last_probe"`
}

func (s *PrefixStat) avgRTT() float64 {
	if s.RTTCount == 0 {
		return 0
	}
	return s.RTTSum / float64(s.RTTCount)
}

// good classifies a prefix against the current network reference (best active
// median, <=0 when unknown). No fixed absolute RTT veto: a cold start must not
// reject every usable prefix. refRTT is supplied by the pool, event-loop only.
func (s *PrefixStat) good(ref float64) bool {
	return s.Probes >= 2 &&
		float64(s.OK)/float64(s.Probes) >= 0.8 &&
		(s.OK == 0 || float64(s.ColoOK)/float64(s.OK) >= 0.5) &&
		(ref <= 0 || s.RTTCount == 0 || s.avgRTT() <= 1.3*ref)
}

func (s *PrefixStat) bad() bool {
	return s.Probes >= 3 &&
		(float64(s.OK)/float64(s.Probes) < 0.3 ||
			(s.OK > 0 && float64(s.ColoOK)/float64(s.OK) < 0.2))
}

// stale means the feedback is too old to describe the current network.
func (s *PrefixStat) stale(now time.Time) bool { return now.Sub(s.LastProbe) > 7*24*time.Hour }

// blockKeyV4 is the one canonical v4 prefix key ("a.b.c", a /24).
func blockKeyV4(b [4]byte) string {
	return fmt.Sprintf("%d.%d.%d", b[0], b[1], b[2])
}

// BlockKey is the prefix-learning / diversity unit: /24 for v4, /48 for v6.
// Record, sampling, diversity and persistence all use this single form.
func BlockKey(addr string) string {
	a, err := netip.ParseAddr(addr)
	if err != nil {
		return addr
	}
	if a.Is4() {
		return blockKeyV4(a.As4())
	}
	return netip.PrefixFrom(a, 48).Masked().Addr().String()
}

type Scanner struct {
	cfg       *Config
	prefixes  []uint32 // /24 base addresses of all CF v4 ranges
	prefixes6 []netip.Prefix
	stats     map[string]*PrefixStat
	known     func(string) bool
	refRTT    func() float64 // best active median, 0 when none; event-loop only
	rnd       *rand.Rand
	noYield   int // consecutive discoveries without a useful candidate
}

func NewScanner(cfg *Config, known func(string) bool, refRTT func() float64) *Scanner {
	s := &Scanner{cfg: cfg, stats: map[string]*PrefixStat{}, known: known, refRTT: refRTT, rnd: rand.New(rand.NewSource(time.Now().UnixNano()))}
	for _, cidr := range cfRanges {
		_, n, err := net.ParseCIDR(cidr)
		if err != nil {
			continue
		}
		base := binary.BigEndian.Uint32(n.IP.To4())
		ones, _ := n.Mask.Size()
		for i := uint32(0); i < 1<<(24-ones); i++ {
			s.prefixes = append(s.prefixes, base+i<<8)
		}
	}
	for _, cidr := range cfRanges6 {
		if p, err := netip.ParsePrefix(cidr); err == nil {
			s.prefixes6 = append(s.prefixes6, p.Masked())
		}
	}
	return s
}

func ipString(v uint32) string {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return net.IP(b).String()
}

// statV4 looks up the canonical /24 stat for a base address.
func (s *Scanner) statV4(base uint32) *PrefixStat {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], base)
	return s.stats[blockKeyV4(b)]
}

func (s *Scanner) Record(addr string, r ProbeResult, now time.Time) {
	if _, err := netip.ParseAddr(addr); err != nil {
		return
	}
	key := BlockKey(addr)
	st, ok := s.stats[key]
	if !ok {
		st = &PrefixStat{Prefix: key}
		s.stats[key] = st
	}
	st.Probes++
	st.LastProbe = now
	if r.Restricted {
		// 1034 is per address space, not per IP: drop the /24's good history
		// so bad() fires now instead of after dozens of fresh failures.
		st.OK, st.ColoOK, st.RTTSum, st.RTTCount = 0, 0, 0, 0
		st.Probes = max(st.Probes, 3)
		return
	}
	if r.OK {
		st.OK++
		st.RTTSum += r.Totalms
		st.RTTCount++
		if s.cfg.coloAllowed(r.Colo) {
			st.ColoOK++
		}
	}
}

// goodV6 returns learned good /48s, including ones inside wider configured
// ranges, so a /32 entry can still sample its learned subprefixes.
func (s *Scanner) goodV6(now time.Time) []netip.Prefix {
	ref := s.refRTT()
	var out []netip.Prefix
	for k, st := range s.stats {
		a, err := netip.ParseAddr(k)
		if err != nil || a.Is4() || st.stale(now) || !st.good(ref) {
			continue
		}
		out = append(out, netip.PrefixFrom(a, 48).Masked())
	}
	return out
}

func (s *Scanner) pickV6() string {
	set := s.prefixes6
	if good := s.goodV6(time.Now()); len(good) > 0 && s.rnd.Float64() < 0.6 {
		set = good
	}
	for range 8 {
		p := set[s.rnd.Intn(len(set))]
		base := p.Addr().As16()
		var rndb [16]byte
		for i := range rndb {
			rndb[i] = byte(s.rnd.Intn(256))
		}
		bits := p.Bits()
		var out [16]byte
		for i := 0; i < 16; i++ {
			keep := 8 * (i + 1)
			switch {
			case keep <= bits:
				out[i] = base[i]
			case 8*i >= bits:
				out[i] = rndb[i]
			default:
				hi := 0xff << (8 - uint(bits-8*i)) & 0xff
				out[i] = base[i]&byte(hi) | rndb[i]&^byte(hi)
			}
		}
		addr := netip.AddrFrom16(out).String()
		if !s.known(addr) {
			return addr
		}
	}
	return ""
}

// pick returns a random candidate IP: 60% historically good /24s, 20%
// unexplored /24s, 10% mediocre, 10% fully random. Stale feedback (>7d)
// is treated as mediocre, not as current knowledge.
func (s *Scanner) pick() string {
	if s.cfg.ScanV6Ratio > 0 && s.rnd.Float64() < s.cfg.ScanV6Ratio {
		return s.pickV6()
	}
	now := time.Now()
	ref := s.refRTT()
	var good, unexplored, mediocre []uint32
	for _, base := range s.prefixes {
		st := s.statV4(base)
		switch {
		case st == nil || st.Probes == 0:
			unexplored = append(unexplored, base)
		case st.stale(now):
			mediocre = append(mediocre, base)
		case st.good(ref):
			good = append(good, base)
		case st.bad():
		default:
			mediocre = append(mediocre, base)
		}
	}
	choose := func(set []uint32) (uint32, bool) {
		if len(set) == 0 {
			return 0, false
		}
		return set[s.rnd.Intn(len(set))], true
	}
	roll := s.rnd.Float64()
	var base uint32
	var ok bool
	switch {
	case roll < 0.6:
		base, ok = choose(good)
		if !ok {
			base, ok = choose(unexplored)
		}
	case roll < 0.8:
		base, ok = choose(unexplored)
	case roll < 0.9:
		base, ok = choose(mediocre)
	default:
		base, ok = choose(s.prefixes)
	}
	if !ok {
		base = s.prefixes[s.rnd.Intn(len(s.prefixes))]
	}
	for range 8 {
		addr := ipString(base + uint32(1+s.rnd.Intn(254)))
		if !s.known(addr) {
			return addr
		}
	}
	return ""
}
