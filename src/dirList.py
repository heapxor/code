import os
from tasks import insertData

def getEmails(sumf, dr, flst):
    for f in flst:	
        fullfile = os.path.join(dr, f)	
        if os.path.isfile(fullfile):
            if not 'envelope' in fullfile:
                size = os.path.getsize(fullfile)
                if size / 1024 < 1024:
                    insertData.delay(fullfile)                    
                    sumf[0] += size
                    sumf[1] += 1
                    
def main():	
    emailStats = [0, 0]
    os.path.walk('./email', getEmails, emailStats)
    print emailStats

if __name__ == '__main__':
    main()