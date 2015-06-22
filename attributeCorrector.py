import re, collections

knownFixes = {}

def attributes(text): return str.split(text)

def train(features):
    model = collections.defaultdict(lambda: 1)
    for f in features:
        model[f] += 1
    return model

NATTRIBUTES = train(attributes(file('big.txt').read()))

alphabet = 'abcdefghijklmnopqrstuvwxyz_0123456789'

def edits1(attribute):
   splits     = [(attribute[:i], attribute[i:]) for i in range(len(attribute) + 1)]
   deletes    = [a + b[1:] for a, b in splits if b]
   transposes = [a + b[1] + b[0] + b[2:] for a, b in splits if len(b)>1]
   replaces   = [a + c + b[1:] for a, b in splits for c in alphabet if b]
   inserts    = [a + c + b     for a, b in splits for c in alphabet]
   return set(deletes + transposes + replaces + inserts)

def known_edits2(attribute):
    return set(e2 for e1 in edits1(attribute) for e2 in edits1(e1) if e2 in NATTRIBUTES)

def known_edits3(attribute):
    return set(e3 for e1 in edits1(attribute) for e2 in edits1(e1) for e3 in edits1(e2) if e3 in NATTRIBUTES)

def known_edits(attribute):
    return

def known(attributes): return set(w for w in attributes if w in NATTRIBUTES)

def correct(attribute):
    # Check if we have seen the given attribute before
    if(attribute not in knownFixes):
      candidates = known([attribute]) or known(edits1(attribute)) or known_edits2(attribute) or known_edits3(attribute) or [attribute]
      correctedAttribute = max(candidates, key=NATTRIBUTES.get)
      
      # update known fixes dict with new link
      knownFixes[attribute] = correctedAttribute
      return correctedAttribute
    else:
      return knownFixes[attribute]

print(correct("air temperatu"))
print(knownFixes)