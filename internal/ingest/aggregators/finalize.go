package aggregators

// Close je namerno no-op da bismo ispoštovali postojeći poziv u main-u.
// Ako ti kasnije zatreba finalna normalizacija, implementiraj ovde.
func (b *AggregateBucket) Close() {}
